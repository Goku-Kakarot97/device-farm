package com.mservice.farmstream.device;

import com.mservice.farmstream.adb.AdbClient;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/** {@code svc power stayon true} for a while, then back off (port of KeepAwakeService.ts). */
public final class KeepAwakeService {

    private final AdbClient adb;
    private final ScheduledExecutorService timers = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "keep-awake");
        t.setDaemon(true);
        return t;
    });
    private final Map<String, ScheduledFuture<?>> pending = new ConcurrentHashMap<>();

    public KeepAwakeService(AdbClient adb) {
        this.adb = adb;
    }

    public void keepAwake(String deviceId, long durationMs) throws IOException {
        if (deviceId == null || deviceId.isBlank()) {
            throw new IOException("Invalid device id");
        }
        long safe = Math.max(1000, durationMs <= 0 ? 30_000 : durationMs);
        adb.shell(deviceId, "svc power stayon true");
        adb.shellSafe(deviceId, "input keyevent 224"); // wake the screen
        ScheduledFuture<?> existing = pending.remove(deviceId);
        if (existing != null) {
            existing.cancel(false);
        }
        pending.put(deviceId, timers.schedule(() -> {
            adb.shellSafe(deviceId, "svc power stayon false");
            pending.remove(deviceId);
        }, safe, TimeUnit.MILLISECONDS));
    }
}
