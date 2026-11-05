package com.mservice.farmstream.device;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

/** Per-device recording state: record / run / stop / pause (port of RecordingStatusService.ts). */
public final class RecordingStatusService {

    public record Status(String device, String status, String recordId, String filePath, String remote, long updatedAt) {
    }

    public record PublicStatus(String device, String status) {
    }

    private final Map<String, Status> statuses = new LinkedHashMap<>();
    private final List<Consumer<List<PublicStatus>>> listeners = new CopyOnWriteArrayList<>();

    public void onSnapshot(Consumer<List<PublicStatus>> listener) {
        listeners.add(listener);
    }

    public void removeListener(Consumer<List<PublicStatus>> listener) {
        listeners.remove(listener);
    }

    public Status setStatus(String device, String status, String recordId, String filePath, String remote) {
        Status full = new Status(device, status, recordId, filePath, remote, System.currentTimeMillis());
        synchronized (this) {
            statuses.put(device, full);
        }
        emit();
        return full;
    }

    public void clearDevice(String device) {
        boolean removed;
        synchronized (this) {
            removed = statuses.remove(device) != null;
        }
        if (removed) {
            emit();
        }
    }

    public synchronized List<Status> getAll() {
        return new ArrayList<>(statuses.values());
    }

    public synchronized List<PublicStatus> getPublicSnapshot() {
        List<PublicStatus> list = new ArrayList<>();
        for (Status s : statuses.values()) {
            list.add(new PublicStatus(s.device(), s.status()));
        }
        return list;
    }

    public synchronized String statusOf(String device) {
        Status s = statuses.get(device);
        return s == null ? null : s.status();
    }

    private void emit() {
        List<PublicStatus> snapshot = getPublicSnapshot();
        for (Consumer<List<PublicStatus>> l : listeners) {
            l.accept(snapshot);
        }
    }
}
