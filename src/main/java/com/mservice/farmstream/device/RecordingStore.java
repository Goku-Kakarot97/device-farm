package com.mservice.farmstream.device;

import com.mservice.farmstream.util.Json;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Recorded control-message sessions as JSON files in {@code recordings/} (port of ActionRecorder.ts + RecordingRepository.ts).
 */
public final class RecordingStore {

    public static final Path ROOT_DIR = Path.of("recordings").toAbsolutePath();

    public record RecordedMessage(long at, String data, boolean binary) {
        public Object decode() {
            return binary ? ByteBuffer.wrap(Base64.getDecoder().decode(data)) : data;
        }
    }

    public record RecordingFile(String id, String remote, String createdAt, String name, Map<String, Object> meta, List<RecordedMessage> messages) {
    }

    public record Listed(String id, String name, String createdAt, String remote, String label) implements Json.Jsonable {
        @Override
        public Object toJson() {
            return Json.obj("id", id, "name", name, "createdAt", createdAt, "remote", remote, "label", label);
        }
    }

    /** Captures messages with their timing while a session is recorded. */
    public static final class Recorder {
        private final String id;
        private final String remote;
        private final Map<String, Object> meta;
        private final long startedAt = System.currentTimeMillis();
        private long pausedAt;
        private long pausedDuration;
        private final List<RecordedMessage> messages = new ArrayList<>();

        public Recorder(String id, String remote, Map<String, Object> meta) {
            this.id = id;
            this.remote = remote;
            this.meta = meta;
        }

        public synchronized void capture(Object data) {
            long at = elapsed();
            if (data instanceof ByteBuffer b) {
                byte[] bytes = new byte[b.remaining()];
                b.duplicate().get(bytes);
                messages.add(new RecordedMessage(at, Base64.getEncoder().encodeToString(bytes), true));
            } else {
                messages.add(new RecordedMessage(at, String.valueOf(data), false));
            }
        }

        public synchronized void pause() {
            if (pausedAt == 0) {
                pausedAt = System.currentTimeMillis();
            }
        }

        public synchronized void resume() {
            if (pausedAt != 0) {
                pausedDuration += System.currentTimeMillis() - pausedAt;
                pausedAt = 0;
            }
        }

        private long elapsed() {
            long pausedDelta = pausedAt != 0 ? System.currentTimeMillis() - pausedAt : 0;
            return System.currentTimeMillis() - startedAt - pausedDuration - pausedDelta;
        }

        public synchronized Path persist() throws IOException {
            Files.createDirectories(ROOT_DIR);
            Path file = ROOT_DIR.resolve(id + ".json");
            List<Object> list = new ArrayList<>();
            for (RecordedMessage m : messages) {
                list.add(Json.obj("at", m.at(), "data", m.data(), "binary", m.binary()));
            }
            Map<String, Object> rec = new LinkedHashMap<>();
            rec.put("id", id);
            rec.put("remote", remote);
            rec.put("createdAt", Instant.ofEpochMilli(startedAt).toString());
            rec.put("name", meta == null ? null : meta.get("name"));
            rec.put("meta", meta);
            rec.put("messages", list);
            Files.writeString(file, Json.stringify(rec), StandardCharsets.UTF_8);
            return file;
        }
    }

    private RecordingStore() {
    }

    /** Sanitises an id; {@code true}/{@code 1} create a fresh one; null/blank → null. */
    public static String normalizeId(String raw) {
        if (raw == null) {
            return null;
        }
        String t = raw.trim();
        if (t.isEmpty()) {
            return null;
        }
        if (t.equals("true") || t.equals("1")) {
            return createId();
        }
        return t.replaceAll("[^a-zA-Z0-9\\-_]", "_");
    }

    public static String createId() {
        return "session-" + System.currentTimeMillis();
    }

    public static RecordingFile load(String id) throws IOException {
        String safe = normalizeId(id);
        if (safe == null) {
            throw new IOException("Recording id is required");
        }
        Path file = ROOT_DIR.resolve(safe + ".json");
        Map<String, Object> parsed = Json.asObject(Json.parse(Files.readString(file, StandardCharsets.UTF_8)));
        List<Object> raw = parsed == null ? null : Json.asList(parsed.get("messages"));
        if (raw == null) {
            throw new IOException("Invalid recording file");
        }
        List<RecordedMessage> messages = new ArrayList<>();
        for (Object o : raw) {
            Map<String, Object> m = Json.asObject(o);
            if (m == null) {
                continue;
            }
            Number at = Json.num(m, "at");
            messages.add(new RecordedMessage(at == null ? 0 : at.longValue(), String.valueOf(m.getOrDefault("data", "")), Json.bool(m, "binary")));
        }
        messages.sort(Comparator.comparingLong(RecordedMessage::at));
        String rid = Json.str(parsed, "id");
        return new RecordingFile(rid == null ? safe : rid, Json.str(parsed, "remote"), Json.str(parsed, "createdAt"), Json.str(parsed, "name"), Json.asObject(parsed.get("meta")), messages);
    }

    public static List<Listed> list() throws IOException {
        List<Listed> records = new ArrayList<>();
        if (!Files.isDirectory(ROOT_DIR)) {
            return records;
        }
        try (Stream<Path> files = Files.list(ROOT_DIR)) {
            for (Path file : files.filter(f -> f.toString().endsWith(".json")).toList()) {
                try {
                    Map<String, Object> parsed = Json.asObject(Json.parse(Files.readString(file, StandardCharsets.UTF_8)));
                    if (parsed == null) {
                        continue;
                    }
                    String base = file.getFileName().toString();
                    String id = Json.str(parsed, "id");
                    if (id == null) {
                        id = base.substring(0, base.length() - 5);
                    }
                    String name = Json.str(parsed, "name");
                    if (name == null) {
                        name = Json.str(Json.asObject(parsed.get("meta")), "name");
                    }
                    String createdAt = Json.str(parsed, "createdAt");
                    if (createdAt == null) {
                        createdAt = Files.getLastModifiedTime(file).toInstant().toString();
                    }
                    records.add(new Listed(id, name, createdAt, Json.str(parsed, "remote"), createdAt + "-" + (name != null ? name : id) + "-false"));
                } catch (RuntimeException | IOException broken) {
                    // skip broken files
                }
            }
        }
        records.sort(Comparator.comparing(Listed::createdAt));
        return records;
    }

    public static void updateName(String recordId, String name) throws IOException {
        Path file = resolve(recordId);
        Map<String, Object> parsed = Json.asObject(Json.parse(Files.readString(file, StandardCharsets.UTF_8)));
        if (parsed == null) {
            throw new IOException("Invalid recording file");
        }
        parsed.put("name", name);
        Files.writeString(file, Json.stringify(parsed), StandardCharsets.UTF_8);
    }

    public static void delete(String recordId) throws IOException {
        Files.delete(resolve(recordId));
    }

    private static Path resolve(String recordId) throws IOException {
        String safe = normalizeId(recordId);
        if (safe == null) {
            throw new IOException("Invalid recordId");
        }
        Path file = ROOT_DIR.resolve(safe + ".json");
        if (!Files.exists(file)) {
            throw new IOException("Recording not found");
        }
        return file;
    }
}
