package com.mservice.farmstream.device;

import com.mservice.farmstream.adb.AdbClient;
import io.hydrangea.EventLoopGroup;
import io.hydrangea.Scheduled;
import io.hydrangea.log.Logger;
import io.hydrangea.websocket.WebSocket;
import io.hydrangea.websocket.WebSocketClient;
import io.hydrangea.websocket.WebSocketConfig;
import io.hydrangea.websocket.WebSocketListener;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * A browser's stream WebSocket proxied to the scrcpy server on a device ({@code ?action=proxy-adb}: through an adb forward
 * of {@code tcp:8886}) or to any WebSocket URL ({@code ?action=proxy-ws}). Messages pass through unchanged in both directions,
 * with recording / replay / pause of the client's control messages and mirroring to synced devices.
 * Port of WebsocketProxy.ts + WebsocketProxyOverAdb.ts.
 */
public final class StreamProxy implements WebSocketListener {

    private static final Logger LOG = Logger.get(StreamProxy.class);
    private static final String TAG = "WebsocketProxy";
    private static final Map<String, StreamProxy> SESSIONS = new ConcurrentHashMap<>();
    private static final WebSocketConfig REMOTE_CONFIG = WebSocketConfig.defaults()
            .withMaxMessageBytes(50 * 1024 * 1024)
            .withHandshakeTimeout(java.time.Duration.ofSeconds(10));

    /** What a proxy needs from the server (services shared by every session). */
    public record Services(EventLoopGroup group, AdbClient adb, RecordingStatusService recording, SyncService sync, DeviceStatusService status) {
    }

    public static StreamProxy getBySession(String sessionId) {
        return SESSIONS.get(sessionId);
    }

    /** {@code ?action=proxy-ws&ws=<url>}. */
    public static StreamProxy direct(Services services, String remoteUrl, String recordId, String replayId, String session) {
        StreamProxy p = new StreamProxy(services, null, null, recordId, replayId, session, null);
        p.pendingRemoteUrl = remoteUrl;
        return p;
    }

    /** {@code ?action=proxy-adb&udid=...&remote=tcp:8886[&path=...][&user=...]}: forwards the port first, on a virtual thread. */
    public static StreamProxy overAdb(Services services, String udid, String remote, String path, String recordId, String replayId, String userEmail) {
        StreamProxy p = new StreamProxy(services, udid, "WebsocketProxyOverAdb", recordId, replayId, null, userEmail);
        p.adbRemote = remote;
        p.adbPath = path == null ? "" : path;
        return p;
    }

    private final Services services;
    private final String device;      // log meta / session id for proxy-adb
    private final String source;
    private final String recordId;
    private final String replayId;
    private final String sessionId;
    private final String userEmail;
    private String pendingRemoteUrl;
    private String adbRemote;
    private String adbPath;

    private WebSocket client;
    private WebSocket remote;
    private final List<Object> storage = new ArrayList<>();
    private String remoteUrl;
    private boolean released;
    private RecordingStore.Recorder recorder;
    private String activeRecordId;
    private RecordingStore.RecordingFile playback;
    private final List<Scheduled> playbackTimers = new ArrayList<>();
    private Scheduled playbackFinishTimer;
    private long playbackStart;
    private long playbackElapsed;
    private boolean recordingPaused;
    private boolean playbackPaused;
    private String currentState = "stop";

    private StreamProxy(Services services, String device, String source, String recordId, String replayId, String session, String userEmail) {
        this.services = services;
        this.device = device;
        this.source = source;
        this.recordId = recordId;
        this.replayId = replayId;
        this.sessionId = session != null ? session : device;
        this.userEmail = userEmail;
        if (sessionId != null) {
            SESSIONS.put(sessionId, this);
        }
    }

    // ---- client side ----

    @Override
    public void onOpen(WebSocket ws) {
        client = ws;
        if (device != null && adbRemote != null) {
            services.status().markInUse(device, userEmail);
            AdbClient.async(() -> {
                try {
                    int port = services.adb().forward(device, adbRemote);
                    init("ws://127.0.0.1:" + port + adbPath);
                } catch (Exception e) {
                    String msg = "[" + TAG + "] Failed to start service: " + e.getMessage();
                    LOG.error(msg);
                    ws.close(4005, truncate(msg));
                }
            });
        } else {
            init(pendingRemoteUrl);
        }
    }

    @Override
    public void onText(WebSocket ws, String text) {
        onSocketMessage(text);
    }

    @Override
    public void onBinary(WebSocket ws, ByteBuffer data) {
        onSocketMessage(data);
    }

    @Override
    public void onClose(WebSocket ws, int code, String reason) {
        if (device != null && adbRemote != null) {
            services.status().markIdle(device, userEmail);
        }
        release();
    }

    private synchronized void onSocketMessage(Object data) {
        if (device != null && LOG.isDebugEnabled()) {
            LOG.debug("[DeviceSocketLog] device={} source={} {}", device, source != null ? source : TAG,
                    data instanceof ByteBuffer b ? "binary " + b.remaining() + " bytes" : "text " + ((String) data).length() + " chars");
        }
        if (recorder != null && !recordingPaused) {
            recorder.capture(data);
        }
        if (remote != null) {
            send(remote, data);
        } else {
            storage.add(data);
        }
        if (sessionId != null) {
            services.sync().mirror(sessionId, data);
        }
    }

    /** A mirrored message from a synced device. */
    public synchronized void forwardFromSync(Object data) {
        if (remote != null) {
            send(remote, data);
        } else {
            storage.add(data);
        }
    }

    private static void send(WebSocket ws, Object data) {
        if (!ws.isOpen()) {
            return;
        }
        if (data instanceof ByteBuffer b) {
            ws.sendBinary(b.duplicate());
        } else {
            ws.sendText((String) data);
        }
    }

    // ---- remote side ----

    private void init(String url) {
        this.remoteUrl = url;
        updateStatus("stop", null, null, 0);
        if (recordId != null) {
            recorder = new RecordingStore.Recorder(recordId, url, meta());
            activeRecordId = recordId;
            updateStatus("record", recordId, null, 0);
        }
        if (replayId != null) {
            try {
                playback = RecordingStore.load(replayId);
            } catch (IOException e) {
                LOG.error("[{}] failed to load recording {}: {}", TAG, replayId, e.getMessage());
            }
        }
        WebSocketClient.connect(services.group(), url, new WebSocketListener() {
            @Override
            public void onOpen(WebSocket ws) {
                synchronized (StreamProxy.this) {
                    remote = ws;
                    flush();
                    startPlayback(null);
                }
            }

            @Override
            public void onText(WebSocket ws, String text) {
                if (client.isOpen()) {
                    client.sendText(text);
                }
            }

            @Override
            public void onBinary(WebSocket ws, ByteBuffer data) {
                if (!client.isOpen()) {
                    return;
                }
                client.sendBinary(data);
                if (!client.isWritable()) { // slow consumer (relay/VPN): hold the device instead of queueing video without bound
                    ws.setAutoRead(false);
                    client.whenWritable().whenComplete((v, e) -> {
                        if (e == null) {
                            ws.setAutoRead(true);
                        } else {
                            ws.close(1000, "");
                        }
                    });
                }
            }

            @Override
            public void onError(WebSocket ws, Throwable error) {
                if (client.isOpen()) {
                    client.close(4011, truncate(String.valueOf(error.getMessage())));
                }
            }

            @Override
            public void onClose(WebSocket ws, int code, String reason) {
                if (client.isOpen()) {
                    client.close(code == 1000 ? 1000 : 4010, "");
                }
            }
        }, REMOTE_CONFIG).whenComplete((ws, err) -> {
            if (err != null) {
                Throwable cause = err.getCause() != null ? err.getCause() : err;
                String msg = "[" + TAG + "] Failed to start service: " + cause.getMessage();
                LOG.error(msg);
                if (client.isOpen()) {
                    client.close(4005, truncate(msg));
                }
            }
        });
    }

    private Map<String, Object> meta() {
        Map<String, Object> m = new LinkedHashMap<>();
        if (device != null) {
            m.put("device", device);
        }
        if (source != null) {
            m.put("source", source);
        }
        return m;
    }

    private void flush() {
        if (remote != null) {
            for (Object data : storage) {
                send(remote, data);
            }
            if (released) {
                remote.close(1000, "");
            }
        }
        storage.clear();
    }

    public void release() {
        synchronized (this) {
            if (released) {
                return;
            }
            released = true;
            if (client != null && client.isOpen()) {
                client.close(1000, "");
            }
            stopPlayback(true);
            persistRecording();
            flush();
        }
        if (sessionId != null) {
            SESSIONS.remove(sessionId, this);
            services.recording().clearDevice(sessionId);
            services.sync().clear();
        }
    }

    // ---- recording ----

    private void persistRecording() {
        if (recorder == null) {
            return;
        }
        try {
            Path file = recorder.persist();
            LOG.info("[{}] recorded actions saved to {}", TAG, file);
        } catch (IOException e) {
            LOG.error("[{}] failed to save recording: {}", TAG, e.getMessage());
        }
    }

    public synchronized String startRecording(String requestedId) throws IOException {
        if (remoteUrl == null) {
            throw new IOException("Cannot start recording before proxy initialization");
        }
        if (!currentState.equals("stop")) {
            throw new IOException("Device is busy; stop current action before starting a new recording");
        }
        if (recorder != null) {
            persistRecording();
        }
        String resolved = RecordingStore.normalizeId(requestedId);
        if (resolved == null) {
            resolved = RecordingStore.createId();
        }
        recorder = new RecordingStore.Recorder(resolved, remoteUrl, meta());
        activeRecordId = resolved;
        updateStatus("record", resolved, null, 0);
        recordingPaused = false;
        return resolved;
    }

    private String stopRecording() throws IOException {
        if (recorder == null) {
            throw new IOException("Nothing to stop");
        }
        if (!currentState.equals("record") && !currentState.equals("pause")) {
            throw new IOException("Recording is not active");
        }
        RecordingStore.Recorder r = recorder;
        recorder = null;
        recordingPaused = false;
        Path file = r.persist();
        String last = activeRecordId;
        activeRecordId = null;
        updateStatus("stop", last, file.toString(), 0);
        return file.toString();
    }

    /** @return {@code {mode, filePath?}} */
    public synchronized Map<String, Object> stop() throws IOException {
        Map<String, Object> result = new LinkedHashMap<>();
        if (recorder != null) {
            result.put("mode", "record");
            result.put("filePath", stopRecording());
            return result;
        }
        if (playback != null) {
            String id = playback.id();
            stopPlayback(true);
            playback = null;
            updateStatus("stop", id, null, 0);
            result.put("mode", "run");
            return result;
        }
        throw new IOException("Nothing to stop");
    }

    public synchronized void runRecording(String id) throws IOException {
        if (id == null || id.isEmpty()) {
            throw new IOException("recordId is required");
        }
        if (!currentState.equals("stop")) {
            throw new IOException("Device is busy; stop current action before running");
        }
        RecordingStore.RecordingFile recording = RecordingStore.load(id);
        stopPlayback(true);
        updateStatus("run", recording.id(), null, 0);
        startPlayback(recording);
    }

    public synchronized String pause() throws IOException {
        if (currentState.equals("pause")) {
            throw new IOException("Already paused");
        }
        if (currentState.equals("stop")) {
            throw new IOException("Nothing to pause");
        }
        if (recorder != null && !recordingPaused) {
            recordingPaused = true;
            recorder.pause();
            updateStatus("pause", activeRecordId, null, 0);
            return "record";
        }
        if (playback != null && !playbackPaused) {
            if (playbackStart != 0) {
                playbackElapsed += System.currentTimeMillis() - playbackStart;
            }
            stopPlayback(false);
            playbackPaused = true;
            updateStatus("pause", playback.id(), null, 0);
            return "run";
        }
        throw new IOException("Nothing to pause");
    }

    public synchronized String resume() throws IOException {
        if (!currentState.equals("pause")) {
            throw new IOException("Cannot resume when not paused");
        }
        if (recorder != null && recordingPaused) {
            recordingPaused = false;
            recorder.resume();
            updateStatus("record", activeRecordId, null, 0);
            return "record";
        }
        if (playback != null && playbackPaused && remote != null && remote.isOpen()) {
            playbackPaused = false;
            schedulePlayback(playback, playbackElapsed);
            return "run";
        }
        throw new IOException("Nothing to resume");
    }

    // ---- playback ----

    private void startPlayback(RecordingStore.RecordingFile recording) {
        RecordingStore.RecordingFile target = recording != null ? recording : playback;
        if (target == null || remote == null || !remote.isOpen()) {
            return;
        }
        playbackElapsed = 0;
        playbackPaused = false;
        playback = target;
        schedulePlayback(target, 0);
    }

    private void stopPlayback(boolean resetProgress) {
        for (Scheduled t : playbackTimers) {
            t.cancel();
        }
        playbackTimers.clear();
        if (playbackFinishTimer != null) {
            playbackFinishTimer.cancel();
            playbackFinishTimer = null;
        }
        playbackStart = 0;
        if (resetProgress) {
            playbackPaused = false;
            playbackElapsed = 0;
        }
    }

    private void schedulePlayback(RecordingStore.RecordingFile recording, long offsetMs) {
        stopPlayback(true);
        playback = recording;
        playbackStart = System.currentTimeMillis();
        updateStatus("run", recording.id(), null, offsetMs);
        for (RecordingStore.RecordedMessage m : recording.messages()) {
            if (m.at() < offsetMs) {
                continue;
            }
            long delay = Math.max(0, m.at() - offsetMs);
            playbackTimers.add(client.eventLoop().schedule(() -> {
                synchronized (this) {
                    if (remote == null || !remote.isOpen()) {
                        return;
                    }
                    try {
                        send(remote, m.decode());
                    } catch (RuntimeException e) {
                        LOG.error("[{}] failed to replay message: {}", TAG, e.getMessage());
                    }
                }
            }, delay, TimeUnit.MILLISECONDS));
        }
    }

    private void updateStatus(String state, String recId, String filePath, long offsetMs) {
        currentState = state;
        if (sessionId == null) {
            return;
        }
        services.recording().setStatus(sessionId, state, recId, filePath, remoteUrl);
        if (state.equals("run") && playback != null) {
            List<RecordingStore.RecordedMessage> msgs = playback.messages();
            long lastAt = msgs.isEmpty() ? 0 : msgs.get(msgs.size() - 1).at();
            long finishMs = Math.max(0, lastAt - offsetMs + 200);
            if (finishMs > 0 && client != null) {
                playbackFinishTimer = client.eventLoop().schedule(() -> {
                    synchronized (this) {
                        updateStatus("stop", recId, null, 0);
                    }
                }, finishMs, TimeUnit.MILLISECONDS);
            }
        }
    }

    private static String truncate(String reason) {
        byte[] b = reason.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        return b.length <= 120 ? reason : new String(b, 0, 120, java.nio.charset.StandardCharsets.UTF_8).replace("�", "");
    }
}
