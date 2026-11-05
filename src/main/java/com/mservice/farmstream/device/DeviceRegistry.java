package com.mservice.farmstream.device;

import com.mservice.farmstream.adb.AdbClient;
import com.mservice.farmstream.adb.AdbServer;
import com.mservice.farmstream.util.Json;
import io.hydrangea.log.Logger;

import java.io.IOException;
import java.net.ConnectException;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

/**
 * The one {@code host:track-devices} loop of the process, and what hangs off it: an {@link AndroidDevice} per serial
 * (properties, scrcpy server), the device status service (label, sdk, connection type, SSID), and change notifications
 * for the device-list sockets. Merges ControlCenter.ts and the tracker half of DeviceListSocket.ts.
 */
public final class DeviceRegistry {

    private static final Logger LOG = Logger.get(DeviceRegistry.class);
    private static final long DEFAULT_WAIT_AFTER_ERROR = 1000;

    public static final String KILL_SERVER = "kill_server";
    public static final String START_SERVER = "start_server";
    public static final String RESTART_SERVER = "restart_server";
    public static final String UPDATE_INTERFACES = "update_interfaces";
    public static final String CONFIGURE_STREAM = "configure_stream";

    private final AdbClient adb;
    private final DeviceStatusService status;
    private final Map<String, AndroidDevice> devices = new ConcurrentHashMap<>();
    private final Map<String, String> lastStates = new HashMap<>();
    private final Map<String, VideoSettings> videoSettings = new ConcurrentHashMap<>();
    private final Map<String, String> ssidCache = new ConcurrentHashMap<>();
    private final List<Consumer<Map<String, Object>>> descriptorListeners = new CopyOnWriteArrayList<>();
    private final List<Runnable> listChangedListeners = new CopyOnWriteArrayList<>();
    private final String id;
    private final String hostname;
    private long waitAfterError = DEFAULT_WAIT_AFTER_ERROR;
    private volatile boolean stopped;

    public DeviceRegistry(AdbClient adb, DeviceStatusService status) {
        this.adb = adb;
        this.status = status;
        String host;
        try {
            host = InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            host = "localhost";
        }
        this.hostname = host;
        String idString = "goog|" + host + "|" + (System.nanoTime() / 1_000_000_000L);
        try {
            this.id = HexFormat.of().formatHex(MessageDigest.getInstance("MD5").digest(idString.getBytes(StandardCharsets.UTF_8)));
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    public String getId() {
        return id;
    }

    public String getName() {
        return "aDevice Tracker [" + hostname + "]";
    }

    /** Descriptor updates of any device (for goog-device-list sockets). */
    public void onDescriptor(Consumer<Map<String, Object>> listener) {
        descriptorListeners.add(listener);
    }

    public void removeDescriptorListener(Consumer<Map<String, Object>> listener) {
        descriptorListeners.remove(listener);
    }

    /** The set of devices or their states changed (for devices-list sockets). */
    public void onListChanged(Runnable listener) {
        listChangedListeners.add(listener);
    }

    public Map<String, String> ssidCache() {
        return ssidCache;
    }

    // ---- tracking ----

    public void start() {
        Thread.ofVirtual().name("adb-tracker").start(this::trackLoop);
    }

    public void stop() {
        stopped = true;
    }

    private void trackLoop() {
        while (!stopped) {
            boolean[] ok = {false};
            boolean[] serverDown = {false};
            adb.trackDevices(new AdbClient.TrackerListener() {
                @Override
                public void onChange(List<AdbClient.DeviceEntry> list) {
                    ok[0] = true;
                    waitAfterError = DEFAULT_WAIT_AFTER_ERROR;
                    applyChangeSet(list);
                }

                @Override
                public void onEnd(Throwable cause) {
                    LOG.warn("device tracker is down ({}); restarting in {}ms", cause == null ? "ended" : cause.getMessage(), waitAfterError);
                    serverDown[0] = cause instanceof ConnectException;
                }
            });
            if (stopped) {
                return;
            }
            if (serverDown[0] && AdbServer.canStart(adb.host()) && AdbServer.start(adb.port())) {
                // the server is back: reconnect right away instead of backing off
                waitAfterError = DEFAULT_WAIT_AFTER_ERROR;
                continue;
            }
            try {
                Thread.sleep(waitAfterError);
            } catch (InterruptedException e) {
                return;
            }
            if (!ok[0]) {
                waitAfterError = Math.min(30_000, (long) (waitAfterError * 1.2));
            }
        }
    }

    private void applyChangeSet(List<AdbClient.DeviceEntry> list) {
        Map<String, String> fresh = new HashMap<>();
        for (AdbClient.DeviceEntry e : list) {
            fresh.put(e.serial(), e.state());
        }
        List<String> added = new ArrayList<>();
        List<String> changed = new ArrayList<>();
        List<String> removed = new ArrayList<>();
        synchronized (lastStates) {
            for (Map.Entry<String, String> e : fresh.entrySet()) {
                String old = lastStates.get(e.getKey());
                if (old == null) {
                    added.add(e.getKey());
                } else if (!old.equals(e.getValue())) {
                    changed.add(e.getKey());
                }
            }
            for (String serial : lastStates.keySet()) {
                if (!fresh.containsKey(serial)) {
                    removed.add(serial);
                }
            }
            lastStates.clear();
            lastStates.putAll(fresh);
        }
        for (String serial : added) {
            String state = fresh.get(serial);
            LOG.info("tracker ADD: id={}, type={}", serial, state);
            handleConnected(serial, state);
            if (isUsable(state)) {
                handleDeviceAdded(serial);
            }
        }
        for (String serial : changed) {
            String state = fresh.get(serial);
            LOG.info("tracker CHANGE: id={}, type={}", serial, state);
            handleConnected(serial, state);
            if (isUsable(state)) {
                handleDeviceAdded(serial);
            }
        }
        for (String serial : removed) {
            LOG.info("tracker REMOVE: id={}", serial);
            handleConnected(serial, "disconnected");
            status.markDisconnected(serial);
            ssidCache.remove(serial);
        }
        if (!added.isEmpty() || !changed.isEmpty() || !removed.isEmpty()) {
            notifyListChanged();
        }
    }

    private static boolean isUsable(String state) {
        return state.equals("device") || state.equals("online");
    }

    private void handleConnected(String udid, String state) {
        AndroidDevice device = devices.get(udid);
        if (device != null) {
            device.setState(state);
        } else {
            devices.put(udid, new AndroidDevice(udid, state, adb, this::onDeviceUpdate));
        }
    }

    private void onDeviceUpdate(AndroidDevice device) {
        Map<String, Object> descriptor = device.descriptor();
        for (Consumer<Map<String, Object>> l : descriptorListeners) {
            try {
                l.accept(descriptor);
            } catch (RuntimeException e) {
                LOG.warn("descriptor listener failed", e);
            }
        }
    }

    /** A usable device appeared: register it at once, then fetch label/sdk and the WiFi SSID in the background. */
    private void handleDeviceAdded(String serial) {
        String connectType = serial.contains(":") ? "wifi" : "usb";
        status.markConnected(serial, connectType, null, null, null);
        AdbClient.async(() -> {
            try {
                Map<String, String> props = adb.getProperties(serial);
                String sdk = props.get("ro.build.version.sdk");
                String model = props.getOrDefault("ro.product.model", "").trim();
                String manufacturer = props.getOrDefault("ro.product.manufacturer", "").trim();
                String label = (manufacturer + " " + model).trim();
                LOG.info("ADB props for {}: label=\"{}\", sdk=\"{}\"", serial, label, sdk);
                status.markConnected(serial, connectType, null, sdk, label.isEmpty() ? null : label);
                notifyListChanged();
            } catch (IOException e) {
                LOG.error("failed to getProperties for {}: {}", serial, e.getMessage());
            }
        });
        AdbClient.async(() -> {
            String line = adb.shellSafe(serial, "wpa_cli -i wlan0 status 2>/dev/null | grep -m1 \"^ssid=\"").trim();
            if (line.startsWith("ssid=")) {
                String ssid = line.substring(5).trim();
                if (!ssid.isEmpty()) {
                    ssidCache.put(serial, ssid);
                    notifyListChanged();
                }
            }
        });
    }

    private void notifyListChanged() {
        for (Runnable r : listChangedListeners) {
            try {
                r.run();
            } catch (RuntimeException e) {
                LOG.warn("list listener failed", e);
            }
        }
    }

    // ---- queries and commands ----

    public List<Map<String, Object>> getDevices() {
        List<Map<String, Object>> list = new ArrayList<>();
        for (AndroidDevice d : devices.values()) {
            list.add(d.descriptor());
        }
        return list;
    }

    public AndroidDevice getDevice(String udid) {
        return devices.get(udid);
    }

    private AndroidDevice require(String udid) throws IOException {
        AndroidDevice device = devices.get(udid);
        if (device == null) {
            throw new IOException("Device with udid:\"" + udid + "\" not found");
        }
        return device;
    }

    private static <T> T withTimeout(CompletableFuture<T> f, long timeoutMs, String label) throws IOException {
        try {
            return f.get(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            throw new IOException(label + " timed out after " + timeoutMs + "ms");
        } catch (ExecutionException e) {
            Throwable c = e.getCause();
            throw c instanceof IOException io ? io : new IOException(c);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("interrupted");
        }
    }

    private static <T> CompletableFuture<T> blocking(Blocking<T> body) {
        CompletableFuture<T> f = new CompletableFuture<>();
        AdbClient.async(() -> {
            try {
                f.complete(body.run());
            } catch (Throwable t) {
                f.completeExceptionally(t);
            }
        });
        return f;
    }

    private interface Blocking<T> {
        T run() throws Exception;
    }

    /** @return the server pid or -1 */
    public int getDevicePid(String udid, long timeoutMs) throws IOException {
        AndroidDevice device = require(udid);
        return withTimeout(blocking(device::getServerPid), timeoutMs, "Get server pid");
    }

    public int restartDevice(String udid, Integer pid, long timeoutMs) throws IOException {
        AndroidDevice device = require(udid);
        int targetPid = pid == null || pid <= 0 ? getDevicePid(udid, Math.min(3000, timeoutMs)) : pid;
        if (targetPid > 0) {
            int p = targetPid;
            withTimeout(blocking(() -> {
                device.killServer(p);
                return null;
            }), Math.min(5000, timeoutMs), "Kill server");
        }
        int started = withTimeout(blocking(device::startServer), timeoutMs, "Start server");
        if (started <= 0) {
            throw new IOException("Server did not start");
        }
        return started;
    }

    public VideoSettings setVideoSettings(String udid, Map<String, Object> s) {
        Map<String, Object> crop = Json.asObject(s.get("crop"));
        Map<String, Object> bounds = Json.asObject(s.get("bounds"));
        VideoSettings v = new VideoSettings(
                crop == null ? null : new VideoSettings.Rect(num(crop, "left"), num(crop, "top"), num(crop, "right"), num(crop, "bottom")),
                num(s, "bitrate"),
                bounds == null ? null : new VideoSettings.Size(num(bounds, "width"), num(bounds, "height")),
                num(s, "maxFps"),
                num(s, "iFrameInterval"),
                Json.bool(s, "sendFrameMeta"),
                s.get("lockedVideoOrientation") instanceof Number n ? n.intValue() : -1,
                num(s, "displayId"),
                Json.str(s, "codecOptions"),
                Json.str(s, "encoderName"));
        videoSettings.put(udid, v);
        return v;
    }

    private static int num(Map<String, Object> m, String key) {
        Object v = m.get(key);
        if (v instanceof Number n) {
            return n.intValue();
        }
        if (v instanceof String str) {
            try {
                return (int) Double.parseDouble(str);
            } catch (NumberFormatException ignored) {
                return 0;
            }
        }
        return 0;
    }

    public VideoSettings getVideoSettings(String udid) {
        return videoSettings.get(udid);
    }

    /** A ControlCenterCommand JSON {@code {id, type, data: {udid, pid}}} from a goog-device-list socket. */
    public void runCommand(Map<String, Object> command) throws IOException {
        String type = Json.str(command, "type");
        Map<String, Object> data = Json.asObject(command.get("data"));
        String udid = Json.str(data, "udid");
        if (type == null || udid == null) {
            throw new IOException("Invalid command");
        }
        AndroidDevice device = devices.get(udid);
        if (device == null) {
            LOG.error("Device with udid:\"{}\" not found", udid);
            return;
        }
        Number pid = Json.num(data, "pid");
        switch (type) {
            case KILL_SERVER -> device.killServer(pid == null ? 0 : pid.intValue());
            case START_SERVER -> device.startServer();
            case RESTART_SERVER -> {
                device.killServer(pid == null ? 0 : pid.intValue());
                device.startServer();
            }
            case UPDATE_INTERFACES -> device.updateInterfaces();
            case CONFIGURE_STREAM -> {
            }
            default -> throw new IOException("Unsupported command: \"" + type + "\"");
        }
    }
}
