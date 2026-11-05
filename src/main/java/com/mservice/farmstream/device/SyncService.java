package com.mservice.farmstream.device;

import com.mservice.farmstream.util.Json;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Mirrors control messages from target devices to other devices' stream sessions (port of SyncService.ts). */
public final class SyncService {

    private final Map<String, Set<String>> mapping = new LinkedHashMap<>();

    public synchronized Map<String, Object> setMapping(List<String> targets, List<String> devices) {
        Set<String> cleanedTargets = new LinkedHashSet<>();
        for (String t : targets) {
            if (t != null && !t.isBlank()) {
                cleanedTargets.add(t.trim());
            }
        }
        Set<String> cleanedDevices = new LinkedHashSet<>();
        for (String d : devices) {
            if (d != null && !d.isBlank()) {
                cleanedDevices.add(d.trim());
            }
        }
        mapping.clear();
        for (String t : cleanedTargets) {
            Set<String> receivers = new LinkedHashSet<>(cleanedDevices);
            receivers.remove(t);
            mapping.put(t, receivers);
        }
        return list();
    }

    public synchronized void clear() {
        mapping.clear();
    }

    public synchronized Map<String, Object> list() {
        List<String> targets = new ArrayList<>();
        Set<String> devices = new LinkedHashSet<>();
        mapping.forEach((t, set) -> {
            targets.add(t);
            devices.addAll(set);
        });
        return Json.obj("targets", targets, "devices", new ArrayList<>(devices));
    }

    public void mirror(String source, Object data) {
        List<String> targets;
        synchronized (this) {
            Set<String> set = mapping.get(source);
            if (set == null || set.isEmpty()) {
                return;
            }
            targets = new ArrayList<>(set);
        }
        for (String device : targets) {
            StreamProxy proxy = StreamProxy.getBySession(device);
            if (proxy != null) {
                proxy.forwardFromSync(data);
            }
        }
    }
}
