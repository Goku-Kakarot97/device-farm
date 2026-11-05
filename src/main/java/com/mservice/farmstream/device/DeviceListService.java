package com.mservice.farmstream.device;

import com.mservice.farmstream.adb.AdbClient;
import com.mservice.farmstream.util.Json;
import io.hydrangea.log.Logger;
import io.hydrangea.websocket.WebSocket;
import io.hydrangea.websocket.WebSocketListener;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The {@code ?action=devices-list} WebSocket: pushes the JSON device list (serial, ip, hardware serial, connection type,
 * recording state, status, label, sdk, owner, SSID) to every subscriber whenever something changes, with a quick cached
 * answer first and a debounced full refresh after. Port of the socket half of DeviceListSocket.ts.
 */
public final class DeviceListService {

    private static final Logger LOG = Logger.get(DeviceListService.class);
    private static final long CACHE_TTL = 5000;
    private static final long IP_NEG_TTL = 30_000;
    private static final long REFRESH_DEBOUNCE = 300;
    private static final List<String> IFACES = List.of("wlan0", "eth0");
    private static final Pattern[] IPV4 = {
            Pattern.compile("\\bsrc\\s+(\\d+\\.\\d+\\.\\d+\\.\\d+)\\b"),
            Pattern.compile("\\binet\\s+(\\d+\\.\\d+\\.\\d+\\.\\d+)/\\d+\\b"),
            Pattern.compile("\\binet\\s+addr:\\s*(\\d+\\.\\d+\\.\\d+\\.\\d+)\\b"),
            Pattern.compile("\\binet\\s+(\\d+\\.\\d+\\.\\d+\\.\\d+)\\b"),
    };

    private record Cached(String value, long ts) {
    }

    private final AdbClient adb;
    private final DeviceRegistry registry;
    private final DeviceStatusService status;
    private final RecordingStatusService recording;
    private final ConnectPreferenceService preference;
    private final Set<WebSocket> sockets = ConcurrentHashMap.newKeySet();
    private final ScheduledExecutorService timer = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "device-list");
        t.setDaemon(true);
        return t;
    });
    private final Map<String, Cached> serialCache = new ConcurrentHashMap<>();
    private final Map<String, Cached> ipCache = new ConcurrentHashMap<>();
    private volatile Cached deviceIdsCache; // value = ids joined by '\n'
    private boolean refreshTimerSet;
    private boolean refreshForce;
    private boolean refreshing;
    private boolean pending;

    public DeviceListService(AdbClient adb, DeviceRegistry registry, DeviceStatusService status, RecordingStatusService recording,
                             ConnectPreferenceService preference) {
        this.adb = adb;
        this.registry = registry;
        this.status = status;
        this.recording = recording;
        this.preference = preference;
        registry.onListChanged(() -> {
            deviceIdsCache = null;
            scheduleRefresh(true);
        });
        status.onChange(list -> scheduleRefresh(false));
    }

    public WebSocketListener listener() {
        return new WebSocketListener() {
            @Override
            public void onOpen(WebSocket ws) {
                sockets.add(ws);
                AdbClient.async(() -> quickSendCachedOrIds());
                scheduleRefresh(true);
            }

            @Override
            public void onText(WebSocket ws, String text) {
                AdbClient.async(() -> quickSendCachedOrIds());
                scheduleRefresh(true);
            }

            @Override
            public void onBinary(WebSocket ws, ByteBuffer data) {
                onText(ws, "");
            }

            @Override
            public void onClose(WebSocket ws, int code, String reason) {
                sockets.remove(ws);
            }
        };
    }

    private void broadcast(List<Map<String, Object>> devices) {
        String json = Json.stringify(devices);
        for (WebSocket ws : sockets) {
            if (ws.isOpen()) {
                ws.sendText(json);
            }
        }
    }

    private void quickSendCachedOrIds() {
        try {
            List<String> ids;
            Cached cached = deviceIdsCache;
            if (cached != null && System.currentTimeMillis() - cached.ts() < CACHE_TTL) {
                ids = cached.value().isEmpty() ? List.of() : List.of(cached.value().split("\n"));
            } else {
                ids = listDevicesFromAdb();
            }
            List<Map<String, Object>> quick = new ArrayList<>();
            for (String id : ids) {
                boolean wifi = id.contains(":");
                quick.add(payload(id, recordingState(id, null), wifi ? parseIp(id) : "", wifi ? "" : id, wifi ? "wifi" : "usb", id));
            }
            broadcast(quick);
        } catch (RuntimeException e) {
            LOG.debug("quick send failed: {}", e.toString());
        }
    }

    private Map<String, Object> payload(String device, String recState, String ipv4, String uuid, String connectType, String lookupKey) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("device", device);
        m.put("status_recodd", recState);
        m.put("ipv4", ipv4 == null ? "" : ipv4);
        m.put("uuid", uuid == null ? "" : uuid);
        m.put("connect_type", connectType);
        m.put("device_status", status.isKnown(lookupKey) ? status.getStatus(lookupKey) : status.getStatus(device));
        m.put("label", status.getLabel(lookupKey) != null ? status.getLabel(lookupKey) : status.getLabel(device));
        m.put("sdk_version", status.getSdkVersion(lookupKey) != null ? status.getSdkVersion(lookupKey) : status.getSdkVersion(device));
        m.put("used_by", status.getCurrentUser(lookupKey) != null ? status.getCurrentUser(lookupKey) : status.getCurrentUser(device));
        Map<String, String> ssid = registry.ssidCache();
        m.put("wifi_ssid", ssid.get(lookupKey) != null ? ssid.get(lookupKey) : ssid.get(device));
        return m;
    }

    private String recordingState(String id, String uuid) {
        String s = recording.statusOf(id);
        if (s == null && uuid != null && !uuid.isEmpty()) {
            s = recording.statusOf(uuid);
        }
        return s == null ? "stop" : s;
    }

    /** The full list, with hardware serials and IPs resolved over ADB (at most 4 devices probed at a time). */
    public List<Map<String, Object>> collectDevices() {
        try {
            List<String> ids = listDeviceIds();
            List<Map<String, Object>> raw = new ArrayList<>();
            Semaphore limit = new Semaphore(4);
            List<Thread> threads = new ArrayList<>();
            Map<String, Object>[] results = newArray(ids.size());
            for (int i = 0; i < ids.size(); i++) {
                String id = ids.get(i);
                int index = i;
                threads.add(Thread.ofVirtual().start(() -> {
                    try {
                        limit.acquire();
                        try {
                            boolean wifi = id.contains(":");
                            String ipv4 = wifi ? parseIp(id) : getDeviceIp(id);
                            String uuid = wifi ? orEmpty(getSerial(id)) : id;
                            String lookupKey = uuid.isEmpty() ? id : uuid;
                            results[index] = payload(id, recordingState(id, uuid), ipv4, uuid, wifi ? "wifi" : "usb", lookupKey);
                        } finally {
                            limit.release();
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }));
            }
            for (Thread t : threads) {
                t.join();
            }
            for (Map<String, Object> r : results) {
                if (r != null) {
                    raw.add(r);
                }
            }
            Map<String, List<Map<String, Object>>> grouped = new LinkedHashMap<>();
            for (Map<String, Object> item : raw) {
                String uuid = (String) item.get("uuid");
                String key = uuid == null || uuid.isEmpty() ? (String) item.get("device") : uuid;
                grouped.computeIfAbsent(key, k -> new ArrayList<>()).add(item);
            }
            List<Map<String, Object>> filtered = new ArrayList<>();
            grouped.forEach((key, list) -> {
                String pref = preference.getPreference(key);
                Map<String, Object> chosen = null;
                if (pref != null) {
                    chosen = list.stream().filter(i -> pref.equals(i.get("connect_type"))).findFirst().orElse(null);
                }
                if (chosen == null) {
                    chosen = list.get(0);
                }
                filtered.add(chosen);
            });
            return filtered;
        } catch (Exception e) {
            LOG.error("failed to list devices: {}", e.toString());
            return List.of();
        }
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object>[] newArray(int n) {
        return (Map<String, Object>[]) new Map[n];
    }

    private static String orEmpty(String s) {
        return s == null ? "" : s;
    }

    private void refreshAndBroadcast(boolean force) {
        if (force) {
            deviceIdsCache = null;
        }
        synchronized (this) {
            if (refreshing) {
                pending = true;
                return;
            }
            refreshing = true;
        }
        try {
            broadcast(collectDevices());
        } finally {
            boolean again;
            synchronized (this) {
                refreshing = false;
                again = pending;
                pending = false;
            }
            if (again) {
                refreshAndBroadcast(false);
            }
        }
    }

    /** Debounced: bursts of tracker/status events collapse into one refresh so adb is not flooded. */
    public void scheduleRefresh(boolean force) {
        synchronized (this) {
            if (force) {
                refreshForce = true;
            }
            if (refreshTimerSet) {
                return;
            }
            refreshTimerSet = true;
        }
        timer.schedule(() -> {
            boolean f;
            synchronized (this) {
                refreshTimerSet = false;
                f = refreshForce;
                refreshForce = false;
            }
            AdbClient.async(() -> refreshAndBroadcast(f));
        }, REFRESH_DEBOUNCE, TimeUnit.MILLISECONDS);
    }

    // ---- adb lookups with caches ----

    private static String parseIp(String id) {
        return id.contains(":") ? id.substring(0, id.indexOf(':')) : id;
    }

    private List<String> listDeviceIds() {
        Cached cached = deviceIdsCache;
        long now = System.currentTimeMillis();
        if (cached != null && now - cached.ts() < CACHE_TTL) {
            return cached.value().isEmpty() ? List.of() : List.of(cached.value().split("\n"));
        }
        List<String> ids = listDevicesFromAdb();
        deviceIdsCache = new Cached(String.join("\n", ids), now);
        return ids;
    }

    private List<String> listDevicesFromAdb() {
        try {
            List<String> ids = new ArrayList<>();
            for (AdbClient.DeviceEntry e : adb.listDevices()) {
                if (e.state().equals("device")) {
                    ids.add(e.serial());
                }
            }
            return ids;
        } catch (IOException e) {
            LOG.error("failed to list devices via adb: {}", e.getMessage());
            return List.of();
        }
    }

    public String getDeviceIp(String serial) {
        Cached cached = ipCache.get(serial);
        long now = System.currentTimeMillis();
        if (cached != null) {
            long ttl = cached.value().isEmpty() ? IP_NEG_TTL : CACHE_TTL;
            if (now - cached.ts() < ttl) {
                return cached.value().isEmpty() ? null : cached.value();
            }
        }
        for (int attempt = 0; attempt < 2; attempt++) {
            String ip = pickIpv4(adb.shellSafe(serial, "ip route get 1.1.1.1"));
            if (ip == null) {
                for (String iface : IFACES) {
                    ip = pickIpv4(adb.shellSafe(serial, "ip -f inet addr show " + iface));
                    if (ip != null) {
                        break;
                    }
                }
            }
            if (ip == null) {
                for (String iface : IFACES) {
                    ip = pickIpv4(adb.shellSafe(serial, "ifconfig " + iface));
                    if (ip != null) {
                        break;
                    }
                }
            }
            if (ip == null) {
                String full = adb.shellSafe(serial, "ifconfig");
                for (String iface : IFACES) {
                    ip = pickIpv4(extractIfaceBlock(full, iface));
                    if (ip != null) {
                        break;
                    }
                }
            }
            if (ip != null) {
                ipCache.put(serial, new Cached(ip, now));
                return ip;
            }
            try {
                Thread.sleep(300);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        ipCache.put(serial, new Cached("", now)); // cache the miss: a device without an IP is not re-probed on every refresh
        return null;
    }

    public String getSerial(String serial) {
        Cached cached = serialCache.get(serial);
        long now = System.currentTimeMillis();
        if (cached != null && now - cached.ts() < CACHE_TTL) {
            return cached.value();
        }
        String value = adb.shellSafe(serial, "getprop ro.serialno").trim();
        if (!value.isEmpty()) {
            serialCache.put(serial, new Cached(value, now));
            return value;
        }
        return null;
    }

    private static String pickIpv4(String text) {
        if (text == null || text.isEmpty()) {
            return null;
        }
        for (Pattern p : IPV4) {
            Matcher m = p.matcher(text);
            if (m.find() && !m.group(1).equals("127.0.0.1")) {
                return m.group(1);
            }
        }
        return null;
    }

    private static String extractIfaceBlock(String ifconfig, String iface) {
        if (ifconfig == null || ifconfig.isEmpty()) {
            return null;
        }
        Matcher m = Pattern.compile("^(?:" + Pattern.quote(iface) + ")\\b[\\s\\S]*?(?=^\\S|\\z)", Pattern.MULTILINE).matcher(ifconfig);
        return m.find() ? m.group() : null;
    }
}
