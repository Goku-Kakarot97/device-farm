package com.mservice.farmstream.device;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** Remembers whether a device (by hardware serial) should be listed through its USB or WiFi connection. */
public final class ConnectPreferenceService {

    private final Map<String, String> preferences = new ConcurrentHashMap<>();

    public void setPreference(String uuid, String connect) {
        if (uuid != null && !uuid.isEmpty()) {
            preferences.put(uuid, connect);
        }
    }

    public String getPreference(String uuid) {
        return uuid == null ? null : preferences.get(uuid);
    }
}
