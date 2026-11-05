package com.mservice.farmstream.device;

import io.hydrangea.log.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

/**
 * In-memory device status (ACTIVE / BUSY / IN_USE / DISCONNECT), ownership and stream counts, mirrored to the database.
 * Port of streaming-device/src/server/services/DeviceStatusService.ts.
 */
public final class DeviceStatusService {

    private static final Logger LOG = Logger.get(DeviceStatusService.class);

    public static final String ACTIVE = "ACTIVE";
    public static final String BUSY = "BUSY";
    public static final String IN_USE = "IN_USE";
    public static final String DISCONNECT = "DISCONNECT";

    public static final class DeviceInfo {
        public final String serial;
        public String label;
        public String sdkVersion;
        public String connectType;
        public String ipv4;
        public String status = DISCONNECT;
        public String currentUser;
        public boolean adbConnected;
        public int streamCount;

        DeviceInfo(String serial) {
            this.serial = serial;
        }
    }

    private final DeviceRepository repo;
    private final Map<String, DeviceInfo> cache = new HashMap<>();
    private final List<Consumer<List<DeviceInfo>>> listeners = new CopyOnWriteArrayList<>();

    public DeviceStatusService(DeviceRepository repo, RecordingStatusService recording) {
        this.repo = repo;
        // recording state changes -> BUSY / un-BUSY
        recording.onSnapshot(snapshot -> {
            boolean changed = false;
            synchronized (this) {
                for (RecordingStatusService.PublicStatus s : snapshot) {
                    DeviceInfo info = cache.get(s.device());
                    if (info == null || !info.adbConnected) {
                        continue;
                    }
                    boolean active = s.status().equals("record") || s.status().equals("run");
                    if (active && !info.status.equals(BUSY)) {
                        info.status = BUSY;
                        repo.setStatus(info.serial, BUSY, false, null);
                        changed = true;
                    } else if (!active && info.status.equals(BUSY)) {
                        info.status = info.streamCount > 0 ? IN_USE : ACTIVE;
                        repo.setStatus(info.serial, info.status, true, info.status.equals(ACTIVE) ? null : info.currentUser);
                        changed = true;
                    }
                }
            }
            if (changed) {
                emitChange();
            }
        });
    }

    public void onChange(Consumer<List<DeviceInfo>> listener) {
        listeners.add(listener);
    }

    /** Loads the stored records; every device starts as DISCONNECT until ADB confirms it. */
    public void init() {
        try {
            repo.ensureTable().get();
            List<DeviceRepository.DeviceRecord> records = repo.findAll().get();
            synchronized (this) {
                for (DeviceRepository.DeviceRecord r : records) {
                    DeviceInfo info = new DeviceInfo(r.serial());
                    info.label = r.label();
                    info.sdkVersion = r.sdkVersion();
                    info.connectType = r.connectType();
                    info.ipv4 = r.ipv4();
                    cache.put(r.serial(), info);
                }
            }
            for (DeviceRepository.DeviceRecord r : records) {
                repo.setStatus(r.serial(), DISCONNECT, false, null);
            }
            LOG.info("loaded {} device(s) from DB", records.size());
        } catch (Exception e) {
            LOG.error("failed to init: {}", e.toString());
        }
    }

    // ---- ADB events ----

    public void markConnected(String serial, String connectType, String ipv4, String sdkVersion, String label) {
        DeviceInfo snapshot;
        synchronized (this) {
            DeviceInfo info = cache.computeIfAbsent(serial, DeviceInfo::new);
            if (label != null) {
                info.label = label;
            }
            if (sdkVersion != null) {
                info.sdkVersion = sdkVersion;
            }
            if (connectType != null) {
                info.connectType = connectType;
            }
            if (ipv4 != null) {
                info.ipv4 = ipv4;
            }
            info.status = info.streamCount > 0 ? IN_USE : ACTIVE;
            if (info.streamCount == 0) {
                info.currentUser = null;
            }
            info.adbConnected = true;
            snapshot = copy(info);
        }
        repo.upsert(serial, snapshot.label, snapshot.sdkVersion, snapshot.connectType, snapshot.ipv4, snapshot.status, snapshot.currentUser, true);
        emitChange();
    }

    public void markDisconnected(String serial) {
        synchronized (this) {
            DeviceInfo info = cache.computeIfAbsent(serial, DeviceInfo::new);
            info.status = DISCONNECT;
            info.currentUser = null;
            info.adbConnected = false;
            info.streamCount = 0;
        }
        repo.setStatus(serial, DISCONNECT, true, null);
        emitChange();
    }

    // ---- stream sessions ----

    /** A stream session opened for this device. */
    public void markInUse(String serial, String userEmail) {
        String status;
        String user;
        synchronized (this) {
            DeviceInfo info = cache.get(serial);
            if (info == null) {
                return;
            }
            info.streamCount++;
            info.status = info.status.equals(BUSY) ? BUSY : IN_USE; // BUSY overrides IN_USE
            if (userEmail != null) {
                info.currentUser = userEmail;
            }
            status = info.status;
            user = info.currentUser;
        }
        repo.setStatus(serial, status, true, user);
        emitChange();
    }

    /** A stream session closed; ownership is released only by the owner (or an anonymous session). */
    public void markIdle(String serial, String userEmail) {
        String status;
        String user;
        synchronized (this) {
            DeviceInfo info = cache.get(serial);
            if (info == null) {
                return;
            }
            if (userEmail != null && info.currentUser != null && !info.currentUser.equals(userEmail)) {
                return;
            }
            info.streamCount = Math.max(0, info.streamCount - 1);
            if (info.streamCount == 0) {
                info.status = info.adbConnected ? ACTIVE : DISCONNECT;
                info.currentUser = null;
            }
            status = info.status;
            user = info.currentUser;
        }
        repo.setStatus(serial, status, true, user);
        emitChange();
    }

    /** Releases a device regardless of ownership (admin / stop-using API). */
    public void forceRelease(String serial) {
        String status;
        synchronized (this) {
            DeviceInfo info = cache.get(serial);
            if (info == null) {
                return;
            }
            info.streamCount = 0;
            info.status = info.adbConnected ? ACTIVE : DISCONNECT;
            info.currentUser = null;
            status = info.status;
        }
        repo.setStatus(serial, status, true, null);
        emitChange();
    }

    // ---- reads ----

    public synchronized String getStatus(String serial) {
        DeviceInfo info = cache.get(serial);
        return info == null ? DISCONNECT : info.status;
    }

    public synchronized String getCurrentUser(String serial) {
        DeviceInfo info = cache.get(serial);
        return info == null ? null : info.currentUser;
    }

    public synchronized String getLabel(String serial) {
        DeviceInfo info = cache.get(serial);
        return info == null ? null : info.label;
    }

    public synchronized String getSdkVersion(String serial) {
        DeviceInfo info = cache.get(serial);
        return info == null ? null : info.sdkVersion;
    }

    public synchronized boolean isKnown(String serial) {
        return cache.containsKey(serial);
    }

    private static DeviceInfo copy(DeviceInfo info) {
        DeviceInfo c = new DeviceInfo(info.serial);
        c.label = info.label;
        c.sdkVersion = info.sdkVersion;
        c.connectType = info.connectType;
        c.ipv4 = info.ipv4;
        c.status = info.status;
        c.currentUser = info.currentUser;
        c.adbConnected = info.adbConnected;
        c.streamCount = info.streamCount;
        return c;
    }

    private void emitChange() {
        List<DeviceInfo> all;
        synchronized (this) {
            all = new ArrayList<>();
            for (DeviceInfo i : cache.values()) {
                all.add(copy(i));
            }
        }
        for (Consumer<List<DeviceInfo>> l : listeners) {
            try {
                l.accept(all);
            } catch (RuntimeException e) {
                LOG.warn("status listener failed", e);
            }
        }
    }
}
