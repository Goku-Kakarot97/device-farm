package com.mservice.farmstream.relay;

import io.hydrangea.EventLoop;
import io.hydrangea.Scheduled;
import io.hydrangea.log.Logger;
import io.hydrangea.websocket.WebSocket;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Relay-server side of one agent's tunnel: opens logical streams over the agent's WebSocket and routes the agent's
 * frames to the {@link Sink} bound to each stream. Frame callbacks run on the tunnel's event loop; sinks are called there.
 */
public final class Tunnel {

    private static final Logger LOG = Logger.get(Tunnel.class);

    /** What a logical stream delivers to its owner (called on the tunnel's event loop). */
    public interface Sink {
        void onData(ByteBuffer data);

        /** The agent closed the stream (target EOF / error) or the whole tunnel went away. */
        void onClose(String reason);
    }

    private record Pending(CompletableFuture<Integer> future, Scheduled timer) {
    }

    public final String id;
    public final Instant connectedAt = Instant.now();
    private final WebSocket ws;
    private final EventLoop loop;
    private final long openTimeoutMs;
    private final AtomicInteger nextStreamId = new AtomicInteger(1);
    private final Map<Integer, Pending> pending = new ConcurrentHashMap<>();
    private final Map<Integer, Sink> streams = new ConcurrentHashMap<>();
    private volatile boolean destroyed;

    Tunnel(String id, WebSocket ws, long openTimeoutMs) {
        this.id = id;
        this.ws = ws;
        this.loop = ws.eventLoop();
        this.openTimeoutMs = openTimeoutMs;
    }

    public int streamCount() {
        return streams.size();
    }

    public WebSocket webSocket() {
        return ws;
    }

    /** Asks the agent to dial {@code target}; completes with the stream id once the agent reports OPEN_OK. */
    public CompletableFuture<Integer> open(String target, boolean tls) {
        int streamId = nextStreamId.getAndIncrement();
        CompletableFuture<Integer> f = new CompletableFuture<>();
        if (destroyed) {
            f.completeExceptionally(new IOException("tunnel closed"));
            return f;
        }
        Scheduled timer = loop.schedule(() -> {
            if (pending.remove(streamId) != null) {
                f.completeExceptionally(new IOException("agent did not open " + target + " within " + openTimeoutMs + "ms"));
            }
        }, openTimeoutMs, TimeUnit.MILLISECONDS);
        pending.put(streamId, new Pending(f, timer));
        String json = "{\"target\":\"" + target + "\",\"tls\":" + tls + "}";
        ws.sendBinary(Frame.encode(Frame.OPEN, streamId, json));
        return f;
    }

    /** Binds the owner of an opened stream. */
    public void attach(int streamId, Sink sink) {
        streams.put(streamId, sink);
        if (destroyed) {
            streams.remove(streamId);
            sink.onClose("tunnel closed");
        }
    }

    public void send(int streamId, ByteBuffer data) {
        if (ws.isOpen()) {
            ws.sendBinary(Frame.encode(Frame.DATA, streamId, data));
        }
    }

    /** Ends the stream from the client side: tells the agent to close the target and forgets the sink. */
    public void close(int streamId) {
        if (streams.remove(streamId) != null && ws.isOpen()) {
            ws.sendBinary(Frame.encode(Frame.CLOSE, streamId));
        }
    }

    void onFrame(ByteBuffer data) {
        Frame frame = Frame.decode(data);
        if (frame == null) {
            return;
        }
        int streamId = frame.streamId();
        switch (frame.type()) {
            case Frame.OPEN_OK, Frame.OPEN_FAIL -> {
                Pending p = pending.remove(streamId);
                if (p == null) {
                    return;
                }
                p.timer.cancel();
                if (frame.type() == Frame.OPEN_OK) {
                    p.future.complete(streamId);
                } else {
                    String msg = frame.payloadText();
                    p.future.completeExceptionally(new IOException(msg.isEmpty() ? "agent failed to connect" : msg));
                }
            }
            case Frame.DATA -> {
                Sink sink = streams.get(streamId);
                if (sink != null) {
                    sink.onData(frame.payload());
                }
            }
            case Frame.CLOSE -> {
                Sink sink = streams.remove(streamId);
                if (sink != null) {
                    sink.onClose(frame.payloadText());
                }
            }
            default -> LOG.debug("tunnel {}: unknown frame type {}", id, frame.type());
        }
    }

    void destroy(String reason) {
        if (destroyed) {
            return;
        }
        destroyed = true;
        LOG.info("tunnel \"{}\" destroyed: {}", id, reason);
        for (Pending p : pending.values()) {
            p.timer.cancel();
            p.future.completeExceptionally(new IOException(reason));
        }
        pending.clear();
        List<Sink> sinks = new ArrayList<>(streams.values());
        streams.clear();
        for (Sink s : sinks) {
            s.onClose(reason);
        }
        if (ws.isOpen()) {
            ws.close(1001, reason.length() > 100 ? reason.substring(0, 100) : reason);
        }
    }
}
