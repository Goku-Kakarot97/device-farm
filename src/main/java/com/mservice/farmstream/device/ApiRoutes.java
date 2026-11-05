package com.mservice.farmstream.device;

import com.mservice.farmstream.adb.AdbClient;
import com.mservice.farmstream.util.Json;
import io.hydrangea.http.HttpResponse;
import io.hydrangea.http.HttpRouter;
import io.hydrangea.http.RouteRequest;
import io.hydrangea.log.Logger;
import io.hydrangea.websocket.WebSocket;
import io.hydrangea.websocket.WebSocketClient;
import io.hydrangea.websocket.WebSocketListener;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * The HTTP JSON API of the device server (port of the express routes in HttpServer.ts). Every route runs its blocking
 * ADB work on a virtual thread and answers with CORS headers.
 */
final class ApiRoutes {

    private static final Logger LOG = Logger.get(ApiRoutes.class);
    private static final Path UPLOAD_DIR = Path.of("uploads").toAbsolutePath();
    private static final Pattern SAFE_NAME = Pattern.compile("[^a-zA-Z0-9_.-]");

    private final AdbClient adb;
    private final DeviceRegistry registry;
    private final DeviceListService deviceList;
    private final DeviceStatusService status;
    private final SyncService sync;
    private final KeepAwakeService keepAwake;
    private final ConnectPreferenceService preference;
    private final StreamProxy.Services services;

    ApiRoutes(AdbClient adb, DeviceRegistry registry, DeviceListService deviceList, DeviceStatusService status, SyncService sync,
              KeepAwakeService keepAwake, ConnectPreferenceService preference, StreamProxy.Services services) {
        this.adb = adb;
        this.registry = registry;
        this.deviceList = deviceList;
        this.status = status;
        this.sync = sync;
        this.keepAwake = keepAwake;
        this.preference = preference;
        this.services = services;
    }

    /** A route body that may block (runs on a virtual thread) and may fail with a status. */
    private interface Body {
        Object run(Map<String, Object> json, RouteRequest r) throws Exception;
    }

    /** An HTTP error with a JSON body. */
    static final class ApiError extends RuntimeException {
        final int code;
        final String field;

        ApiError(int code, String message) {
            this(code, message, "error");
        }

        ApiError(int code, String message, String field) {
            super(message, null, false, false);
            this.code = code;
            this.field = field;
        }
    }

    /** A successful answer that carries its own status (207 for partial results). */
    record Answer(int code, Object body) {
    }

    static HttpResponse cors(HttpResponse r) {
        return r.header("Access-Control-Allow-Origin", "*")
                .header("Access-Control-Allow-Methods", "GET,POST,OPTIONS")
                .header("Access-Control-Allow-Headers", "Content-Type, X-UDID, X-Filename, X-File-Size");
    }

    static HttpResponse json(int code, Object body) {
        return cors(HttpResponse.status(code).header("Content-Type", "application/json; charset=utf-8")
                .body(Json.stringify(body).getBytes(StandardCharsets.UTF_8)));
    }

    private CompletionStage<HttpResponse> run(RouteRequest r, Body body) {
        CompletableFuture<HttpResponse> f = new CompletableFuture<>();
        AdbClient.async(() -> {
            try {
                Map<String, Object> json = Map.of();
                String type = r.header("content-type");
                if (r.body().length > 0 && (type == null || type.toLowerCase(Locale.ROOT).contains("json"))) {
                    Object parsed;
                    try {
                        parsed = Json.parse(r.bodyAsString());
                    } catch (RuntimeException e) {
                        throw new ApiError(400, "Invalid JSON body");
                    }
                    Map<String, Object> obj = Json.asObject(parsed);
                    json = obj != null ? obj : Map.of("__array", parsed);
                }
                Object result = body.run(json, r);
                if (result instanceof Answer a) {
                    f.complete(json(a.code(), a.body()));
                } else {
                    f.complete(json(200, result));
                }
            } catch (ApiError e) {
                f.complete(json(e.code, Json.obj("success", false, e.field, e.getMessage())));
            } catch (Throwable t) {
                LOG.warn("api {} failed: {}", r.path(), t.toString());
                f.complete(json(500, Json.obj("success", false, "error", t.getMessage() == null ? t.toString() : t.getMessage())));
            }
        });
        return f;
    }

    private static String requireString(Map<String, Object> json, String key, String error) {
        String v = Json.str(json, key);
        if (v == null || v.trim().isEmpty()) {
            throw new ApiError(400, error);
        }
        return v.trim();
    }

    /** {@code udids: [...]} or {@code udid: "..."} → the non-empty trimmed list. */
    private static List<String> udids(Map<String, Object> json) {
        List<String> out = new ArrayList<>();
        List<Object> list = Json.asList(json.get("udids"));
        if (list != null) {
            for (Object o : list) {
                if (o instanceof String s && !s.trim().isEmpty()) {
                    out.add(s.trim());
                }
            }
        } else {
            String one = Json.str(json, "udid");
            if (one != null && !one.trim().isEmpty()) {
                out.add(one.trim());
            }
        }
        return out;
    }

    void register(HttpRouter router) {
        // ---- scrcpy server on a device ----
        router.postAsync("/api/goog/device/pid", r -> run(r, (json, rr) -> {
            String udid = requireString(json, "udid", "Invalid \"udid\"");
            int pid = registry.getDevicePid(udid, 5000);
            if (pid <= 0) {
                throw new ApiError(404, "Server PID not found");
            }
            return Json.obj("success", true, "pid", pid);
        }));
        router.postAsync("/api/goog/device/config", r -> run(r, (json, rr) -> {
            String udid = requireString(json, "udid", "Invalid \"udid\"");
            Map<String, Object> settings = Json.asObject(json.get("videoSettings"));
            if (settings == null) {
                throw new ApiError(400, "Invalid \"videoSettings\"");
            }
            Map<String, Object> crop = Json.asObject(settings.get("crop"));
            if (settings.get("crop") != null && crop == null) {
                throw new ApiError(400, "Invalid crop");
            }
            if (crop != null) {
                for (String k : List.of("left", "top", "right", "bottom")) {
                    if (!(crop.get(k) instanceof Number)) {
                        throw new ApiError(400, "Invalid crop." + k);
                    }
                }
            }
            Map<String, Object> bounds = Json.asObject(settings.get("bounds"));
            if (bounds != null && !(bounds.get("width") instanceof Number && bounds.get("height") instanceof Number)) {
                throw new ApiError(400, "Invalid bounds (width/height)");
            }
            VideoSettings v = registry.setVideoSettings(udid, settings);
            return Json.obj("success", true, "videoSettings", v);
        }));
        router.postAsync("/api/goog/device/restart", r -> run(r, (json, rr) -> {
            List<String> targets = udids(json);
            if (targets.isEmpty()) {
                throw new ApiError(400, "Invalid \"udids\" or \"udid\"");
            }
            Number pid = Json.num(json, "pid");
            List<Map<String, Object>> results = parallel(targets, udid -> {
                try {
                    int newPid = registry.restartDevice(udid, pid == null ? null : pid.intValue(), 20_000);
                    return Json.obj("udid", udid, "success", true, "pid", newPid);
                } catch (IOException e) {
                    return Json.obj("udid", udid, "success", false, "error", e.getMessage());
                }
            });
            boolean all = results.stream().allMatch(m -> Boolean.TRUE.equals(m.get("success")));
            Map<String, Object> response = Json.obj("success", all, "results", results);
            if (results.size() == 1 && all) {
                response.put("pid", results.get(0).get("pid"));
            }
            return new Answer(all ? 200 : 207, response);
        }));
        router.postAsync("/api/goog/device/send-binary", r -> run(r, (json, rr) -> {
            List<String> targets = udids(json);
            if (targets.isEmpty()) {
                throw new ApiError(400, "Invalid \"udids\" or \"udid\"");
            }
            String remote = json.get("remote") == null ? "tcp:8886" : Json.str(json, "remote");
            if (remote == null || remote.isEmpty()) {
                throw new ApiError(400, "Invalid \"remote\"");
            }
            String dataBase64 = Json.str(json, "dataBase64");
            if (dataBase64 == null || dataBase64.isEmpty()) {
                throw new ApiError(400, "Invalid \"dataBase64\"");
            }
            byte[] data;
            try {
                data = Base64.getDecoder().decode(dataBase64);
            } catch (IllegalArgumentException e) {
                throw new ApiError(400, "Invalid base64");
            }
            if (data.length == 0) {
                throw new ApiError(400, "Empty buffer");
            }
            String wsPath = Json.str(json, "path");
            Number timeout = Json.num(json, "timeoutMs");
            long timeoutMs = timeout != null && timeout.longValue() > 0 ? timeout.longValue() : 5000;
            List<Map<String, Object>> results = parallel(targets, udid -> {
                try {
                    int port = adb.forward(udid, remote);
                    sendBinaryOnce("ws://127.0.0.1:" + port + (wsPath == null ? "" : wsPath), data, timeoutMs);
                    return Json.obj("udid", udid, "success", true);
                } catch (Exception e) {
                    return Json.obj("udid", udid, "success", false, "error", e.getMessage() == null ? e.toString() : e.getMessage());
                }
            });
            boolean all = results.stream().allMatch(m -> Boolean.TRUE.equals(m.get("success")));
            return new Answer(all ? 200 : 207, Json.obj("success", all, "results", results));
        }));

        // ---- apk upload and install ----
        router.postAsync("/api/goog/device/install-apk", r -> run(r, (json, rr) -> {
            requireString(json, "udid", "Invalid \"udid\"");
            String dataBase64 = Json.str(json, "dataBase64");
            if (dataBase64 == null || dataBase64.isEmpty()) {
                throw new ApiError(400, "Invalid \"dataBase64\"");
            }
            byte[] data;
            try {
                data = Base64.getDecoder().decode(dataBase64);
            } catch (IllegalArgumentException e) {
                throw new ApiError(400, "Invalid base64");
            }
            if (data.length == 0) {
                throw new ApiError(400, "Empty buffer");
            }
            Path tmp = saveUpload(Json.str(json, "fileName"), data);
            return Json.obj("success", true, "filePath", tmp.toString());
        }));
        router.postAsync("/api/goog/device/install-apk-binary", r -> run(r, (json, rr) -> {
            String udid = first(rr.header("x-udid"), rr.param("udid"));
            if (udid == null || udid.trim().isEmpty()) {
                throw new ApiError(400, "Invalid \"udid\"");
            }
            byte[] data = rr.body();
            if (data.length == 0) {
                throw new ApiError(400, "Empty apk payload");
            }
            String expected = rr.header("x-file-size");
            if (expected != null) {
                try {
                    long size = Long.parseLong(expected.trim());
                    if (size != data.length) {
                        throw new ApiError(400, "Size mismatch: got " + data.length + ", expected " + size);
                    }
                } catch (NumberFormatException ignored) {
                    // header not a number: ignored like the Node version
                }
            }
            Path tmp = saveUpload(first(rr.header("x-filename"), rr.param("fileName")), data);
            return Json.obj("success", true, "filePath", tmp.toString());
        }));
        router.postAsync("/api/goog/device/install-uploaded", r -> run(r, (json, rr) -> {
            String udid = requireString(json, "udid", "Invalid \"udid\"");
            String filePath = requireString(json, "filePath", "Invalid \"filePath\"");
            Path resolved = Path.of(filePath).toAbsolutePath().normalize();
            if (!resolved.startsWith(UPLOAD_DIR)) {
                throw new ApiError(400, "filePath not allowed");
            }
            if (!Files.exists(resolved)) {
                throw new ApiError(404, "file not found");
            }
            String output = installApk(udid, resolved);
            return Json.obj("success", true, "output", output);
        }));

        // ---- recordings ----
        router.postAsync("/api/recordings/start", r -> run(r, (json, rr) -> {
            StreamProxy proxy = sessionProxy(json);
            try {
                return Json.obj("success", true, "recordId", proxy.startRecording(Json.str(json, "recordId")));
            } catch (IOException e) {
                throw new ApiError(400, e.getMessage(), "message");
            }
        }));
        router.postAsync("/api/recordings/stop", r -> run(r, (json, rr) -> {
            StreamProxy proxy = sessionProxy(json);
            try {
                Map<String, Object> result = new LinkedHashMap<>();
                result.put("success", true);
                result.putAll(proxy.stop());
                return result;
            } catch (IOException e) {
                throw new ApiError(400, e.getMessage(), "message");
            }
        }));
        router.postAsync("/api/recordings/run", r -> run(r, (json, rr) -> {
            String resolved = RecordingStore.normalizeId(Json.str(json, "recordId"));
            String session = Json.str(json, "session");
            if (session == null || session.trim().isEmpty()) {
                throw new ApiError(400, "Invalid \"session\"", "message");
            }
            if (resolved == null) {
                throw new ApiError(400, "Invalid \"recordId\"", "message");
            }
            StreamProxy proxy = sessionProxy(json);
            try {
                proxy.runRecording(resolved);
                return Json.obj("success", true);
            } catch (IOException e) {
                throw new ApiError(400, e.getMessage(), "message");
            }
        }));
        router.postAsync("/api/recordings/pause", r -> run(r, (json, rr) -> {
            StreamProxy proxy = sessionProxy(json);
            try {
                return Json.obj("success", true, "mode", proxy.pause());
            } catch (IOException e) {
                throw new ApiError(400, e.getMessage(), "message");
            }
        }));
        router.postAsync("/api/recordings/resume", r -> run(r, (json, rr) -> {
            StreamProxy proxy = sessionProxy(json);
            try {
                return Json.obj("success", true, "mode", proxy.resume());
            } catch (IOException e) {
                throw new ApiError(400, e.getMessage(), "message");
            }
        }));
        router.getAsync("/api/recordings", r -> run(r, (json, rr) -> {
            try {
                return Json.obj("success", true, "records", RecordingStore.list());
            } catch (IOException e) {
                throw new ApiError(500, e.getMessage(), "message");
            }
        }));
        router.postAsync("/api/recordings/update-name", r -> run(r, (json, rr) -> {
            String recordId = Json.str(json, "recordId");
            String name = Json.str(json, "name");
            if (recordId == null || recordId.trim().isEmpty()) {
                throw new ApiError(400, "Invalid \"recordId\"", "message");
            }
            if (name == null || name.trim().isEmpty()) {
                throw new ApiError(400, "Invalid \"name\"", "message");
            }
            try {
                RecordingStore.updateName(recordId, name.trim());
                return Json.obj("success", true);
            } catch (IOException e) {
                throw new ApiError(400, e.getMessage(), "message");
            }
        }));
        router.postAsync("/api/recordings/delete", r -> run(r, (json, rr) -> {
            String recordId = Json.str(json, "recordId");
            if (recordId == null || recordId.trim().isEmpty()) {
                throw new ApiError(400, "Invalid \"recordId\"", "message");
            }
            try {
                RecordingStore.delete(recordId.trim());
                return Json.obj("success", true);
            } catch (IOException e) {
                throw new ApiError(400, e.getMessage(), "message");
            }
        }));

        // ---- devices ----
        router.postAsync("/api/devices/connect", r -> run(r, (json, rr) -> {
            List<Object> payloads = json.containsKey("__array") ? Json.asList(json.get("__array")) : List.of(json);
            if (payloads == null || payloads.isEmpty()) {
                throw new ApiError(400, "Empty payload");
            }
            List<Map<String, Object>> results = parallel(payloads, this::switchConnection);
            List<Map<String, Object>> devices = deviceList.collectDevices();
            boolean all = results.stream().allMatch(m -> Boolean.TRUE.equals(m.get("success")));
            return Json.obj("success", all, "results", results, "devices", devices);
        }));
        router.postAsync("/api/device/keep-awake", r -> run(r, (json, rr) -> {
            String device = requireString(json, "device", "Invalid \"device\"");
            Number seconds = Json.num(json, "seconds");
            long durationMs = seconds != null && seconds.longValue() > 0 ? seconds.longValue() * 1000 : 30_000;
            keepAwake.keepAwake(device, durationMs);
            return Json.obj("success", true, "device", device, "durationMs", durationMs);
        }));
        router.postAsync("/api/device/stop-using", r -> run(r, (json, rr) -> {
            String serial = requireString(json, "serial", "Invalid \"serial\"");
            String user = Json.str(json, "user");
            String owner = status.getCurrentUser(serial);
            if (user == null || user.isEmpty() || owner == null || owner.equals(user)) {
                status.forceRelease(serial);
                return Json.obj("success", true);
            }
            throw new ApiError(403, "Device is used by another user");
        }));

        // ---- sync ----
        router.getAsync("/api/sync", r -> run(r, (json, rr) -> Json.obj("success", true, "sync", sync.list())));
        router.postAsync("/api/sync/set", r -> run(r, (json, rr) -> {
            Object target = json.get("target_device");
            List<String> targets = new ArrayList<>();
            if (target instanceof String s) {
                targets.add(s);
            } else if (target instanceof List<?> l) {
                for (Object o : l) {
                    if (o instanceof String s) {
                        targets.add(s);
                    }
                }
            }
            targets.removeIf(t -> t == null || t.isBlank());
            if (targets.isEmpty()) {
                throw new ApiError(400, "Invalid \"target_device\"", "message");
            }
            List<Object> syncDevices = Json.asList(json.get("sync_devices"));
            if (syncDevices == null) {
                throw new ApiError(400, "Invalid \"sync_devices\"", "message");
            }
            List<String> devices = new ArrayList<>();
            for (Object o : syncDevices) {
                if (o instanceof String s && !s.isBlank()) {
                    devices.add(s.trim());
                }
            }
            return Json.obj("success", true, "sync", sync.setMapping(targets, devices));
        }));
        router.postAsync("/api/sync/clear", r -> run(r, (json, rr) -> {
            sync.clear();
            return Json.obj("success", true);
        }));

        // ---- CORS preflight ----
        router.route("OPTIONS", "/{path...}", r -> cors(HttpResponse.status(204)));
        router.route("OPTIONS", "/", r -> cors(HttpResponse.status(204)));
    }

    private static String first(String a, String b) {
        return a != null && !a.isEmpty() ? a : b;
    }

    private StreamProxy sessionProxy(Map<String, Object> json) {
        String session = Json.str(json, "session");
        if (session == null || session.trim().isEmpty()) {
            throw new ApiError(400, "Invalid \"session\"", "message");
        }
        StreamProxy proxy = StreamProxy.getBySession(session.trim());
        if (proxy == null) {
            throw new ApiError(404, "Session not found", "message");
        }
        return proxy;
    }

    private interface Work<T> {
        Map<String, Object> run(T item);
    }

    private static <T> List<Map<String, Object>> parallel(List<T> items, Work<T> work) throws InterruptedException {
        List<Map<String, Object>> results = new ArrayList<>();
        List<Thread> threads = new ArrayList<>();
        Map<String, Object>[] out = newArray(items.size());
        for (int i = 0; i < items.size(); i++) {
            int index = i;
            T item = items.get(i);
            threads.add(Thread.ofVirtual().start(() -> out[index] = work.run(item)));
        }
        for (Thread t : threads) {
            t.join();
        }
        for (Map<String, Object> m : out) {
            results.add(m);
        }
        return results;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object>[] newArray(int n) {
        return (Map<String, Object>[]) new Map[n];
    }

    private void sendBinaryOnce(String url, byte[] data, long timeoutMs) throws Exception {
        CompletableFuture<Void> closed = new CompletableFuture<>();
        WebSocket ws = WebSocketClient.connect(services.group(), url, new WebSocketListener() {
            @Override
            public void onClose(WebSocket ws, int code, String reason) {
                closed.complete(null);
            }

            @Override
            public void onError(WebSocket ws, Throwable error) {
                closed.completeExceptionally(error);
            }
        }).get(timeoutMs, TimeUnit.MILLISECONDS);
        ws.sendBinary(ByteBuffer.wrap(data)).get(timeoutMs, TimeUnit.MILLISECONDS);
        ws.close(1000, "");
        try {
            closed.get(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (java.util.concurrent.TimeoutException e) {
            throw new IOException("Timed out waiting for websocket");
        }
    }

    private static Path saveUpload(String fileName, byte[] data) throws IOException {
        String safe = fileName != null && !fileName.trim().isEmpty() ? SAFE_NAME.matcher(fileName.trim()).replaceAll("_") : "upload.apk";
        Files.createDirectories(UPLOAD_DIR);
        Path tmp = UPLOAD_DIR.resolve("ws-scrcpy-upload-" + System.currentTimeMillis() + "-" + Long.toHexString(Double.doubleToLongBits(Math.random())) + "-" + safe);
        Files.write(tmp, data);
        return tmp;
    }

    /** {@code adb install -r} for an apk, or a split install for an xapk/zip bundle; the uploaded file is removed afterwards. */
    private String installApk(String udid, Path apkPath) throws IOException {
        String name = apkPath.getFileName().toString().toLowerCase(Locale.ROOT);
        if (name.endsWith(".xapk") || name.endsWith(".zip")) {
            Path tmpDir = Files.createTempDirectory("ws-scrcpy-xapk-");
            try {
                List<Path> apks = unzipApks(apkPath, tmpDir);
                if (apks.isEmpty()) {
                    throw new IOException("No APK files found in xapk/zip");
                }
                apks.sort(Comparator.comparing((Path p) -> !p.getFileName().toString().toLowerCase(Locale.ROOT).contains("base"))
                        .thenComparing(p -> p.getFileName().toString().toLowerCase(Locale.ROOT)));
                return installSplits(udid, apks);
            } finally {
                deleteTree(tmpDir);
            }
        }
        String remote = "/data/local/tmp/" + apkPath.getFileName();
        try {
            adb.push(udid, apkPath, remote);
            String out = adb.shell(udid, "pm install -r " + remote, 180_000).trim();
            if (out.toLowerCase(Locale.ROOT).contains("success")) {
                return out.isEmpty() ? "Success" : out;
            }
            throw new IOException(out.isEmpty() ? "pm install failed" : out);
        } finally {
            adb.shellSafe(udid, "rm " + remote);
        }
    }

    private String installSplits(String udid, List<Path> apks) throws IOException {
        String created = adb.shell(udid, "pm install-create -r").trim();
        Matcher m = Pattern.compile("\\[(\\d+)\\]").matcher(created);
        if (!m.find()) {
            throw new IOException(created.isEmpty() ? "pm install-create failed" : created);
        }
        String session = m.group(1);
        List<String> pushed = new ArrayList<>();
        try {
            int i = 0;
            for (Path apk : apks) {
                String remote = "/data/local/tmp/split-" + session + "-" + (i++) + ".apk";
                adb.push(udid, apk, remote);
                pushed.add(remote);
                String out = adb.shell(udid, "pm install-write -S " + Files.size(apk) + " " + session + " " + i + ".apk " + remote, 180_000).trim();
                if (!out.toLowerCase(Locale.ROOT).contains("success")) {
                    throw new IOException(out.isEmpty() ? "pm install-write failed" : out);
                }
            }
            String out = adb.shell(udid, "pm install-commit " + session, 180_000).trim();
            if (!out.toLowerCase(Locale.ROOT).contains("success")) {
                throw new IOException(out.isEmpty() ? "pm install-commit failed" : out);
            }
            return out;
        } catch (IOException e) {
            adb.shellSafe(udid, "pm install-abandon " + session);
            throw e;
        } finally {
            for (String remote : pushed) {
                adb.shellSafe(udid, "rm " + remote);
            }
        }
    }

    private static List<Path> unzipApks(Path zip, Path dir) throws IOException {
        List<Path> apks = new ArrayList<>();
        try (ZipInputStream in = new ZipInputStream(Files.newInputStream(zip))) {
            ZipEntry entry;
            while ((entry = in.getNextEntry()) != null) {
                Path target = dir.resolve(entry.getName()).normalize();
                if (!target.startsWith(dir)) {
                    throw new IOException("zip entry outside target dir: " + entry.getName());
                }
                if (entry.isDirectory()) {
                    Files.createDirectories(target);
                    continue;
                }
                Files.createDirectories(target.getParent());
                Files.copy(in, target);
                if (entry.getName().toLowerCase(Locale.ROOT).endsWith(".apk")) {
                    apks.add(target);
                }
            }
        }
        return apks;
    }

    private static void deleteTree(Path dir) {
        try (Stream<Path> walk = Files.walk(dir)) {
            walk.sorted(Comparator.reverseOrder()).forEach(p -> {
                try {
                    Files.deleteIfExists(p);
                } catch (IOException ignored) {
                    // best effort
                }
            });
        } catch (IOException ignored) {
            // best effort
        }
    }

    /** One item of POST /api/devices/connect: {@code {device, connect: usb|wifi, port?}}. */
    private Map<String, Object> switchConnection(Object itemObj) {
        Map<String, Object> item = Json.asObject(itemObj);
        String device = item == null ? null : Json.str(item, "device");
        String connect = item == null ? null : Json.str(item, "connect");
        String deviceStr = device == null ? "" : device.trim();
        String connectType = "wifi".equals(connect) ? "wifi" : "usb".equals(connect) ? "usb" : null;
        if (deviceStr.isEmpty() || connectType == null) {
            return Json.obj("device", deviceStr, "connect", connect, "success", false, "error", "Invalid \"device\" or \"connect\"");
        }
        try {
            if (connectType.equals("usb")) {
                String uuid = deviceStr.contains(":") ? deviceList.getSerial(deviceStr) : deviceStr;
                if (uuid == null) {
                    throw new IOException("Unable to resolve device uuid");
                }
                adb.usb(uuid);
                for (Map<String, Object> d : deviceList.collectDevices()) {
                    String id = (String) d.get("device");
                    if (uuid.equals(d.get("uuid")) && "wifi".equals(d.get("connect_type")) && id.contains(":")) {
                        try {
                            adb.disconnect(id);
                        } catch (IOException ignored) {
                            // best effort
                        }
                    }
                }
                if (deviceStr.contains(":")) {
                    try {
                        adb.disconnect(deviceStr);
                    } catch (IOException ignored) {
                        // best effort
                    }
                }
                preference.setPreference(uuid, "usb");
            } else {
                Number portRaw = item.get("port") instanceof Number n && n.intValue() > 0 ? n : null;
                Integer portFromDevice = null;
                if (deviceStr.contains(":")) {
                    try {
                        portFromDevice = Integer.parseInt(deviceStr.substring(deviceStr.indexOf(':') + 1));
                    } catch (NumberFormatException ignored) {
                        portFromDevice = null;
                    }
                }
                int targetPort = portRaw != null ? portRaw.intValue() : portFromDevice != null ? portFromDevice : 5555;
                String uuid = deviceStr.contains(":") ? deviceList.getSerial(deviceStr) : deviceStr;
                if (uuid == null) {
                    throw new IOException("Unable to resolve device uuid");
                }
                String host = deviceStr.contains(":") ? deviceStr.substring(0, deviceStr.indexOf(':')) : deviceList.getDeviceIp(uuid);
                if (host == null) {
                    throw new IOException("Unable to resolve device ip");
                }
                String target = host + ":" + targetPort;
                long deadline = System.currentTimeMillis() + 10_000;
                adb.tcpip(uuid, targetPort);
                Thread.sleep(400);
                boolean connected = false;
                String lastError = null;
                for (int i = 0; i < 3 && !connected; i++) {
                    if (System.currentTimeMillis() > deadline) {
                        throw new IOException("WiFi connection timed out after 10 seconds");
                    }
                    try {
                        String out = adb.connect(target);
                        lastError = out;
                        String lower = out.toLowerCase(Locale.ROOT);
                        if (lower.contains("connected to") || lower.contains("already connected")) {
                            connected = true;
                            break;
                        }
                    } catch (IOException e) {
                        lastError = e.getMessage();
                    }
                    Thread.sleep(400);
                }
                if (!connected) {
                    throw new IOException(lastError == null ? "Failed to connect over WiFi" : lastError);
                }
                preference.setPreference(uuid, "wifi");
            }
            return Json.obj("device", deviceStr, "connect", connectType, "success", true);
        } catch (Exception e) {
            return Json.obj("device", deviceStr, "connect", connectType, "success", false,
                    "error", e.getMessage() == null ? "Failed to switch connection" : e.getMessage());
        }
    }

    static byte[] readResource(String name) throws IOException {
        try (InputStream in = ApiRoutes.class.getResourceAsStream(name)) {
            return in == null ? null : in.readAllBytes();
        }
    }
}
