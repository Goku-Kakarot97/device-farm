package com.mservice.farmstream.relay;

import io.hydrangea.Connection;
import io.hydrangea.http.Headers;
import io.hydrangea.http.HttpResponse;
import io.hydrangea.log.Logger;
import io.hydrangea.pipeline.Context;
import io.hydrangea.pipeline.Stage;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * One proxied client request on the relay-server. The upstream's raw bytes arrive from the agent (on the tunnel's loop);
 * the response head is parsed here, then:
 * <ul>
 *   <li>a {@code 101} (WebSocket upgrade) or a {@code 2xx} becomes a takeover response: the client connection is handed to a
 *       {@link PipeStage} that pipes raw bytes both ways until either side closes;</li>
 *   <li>anything else is buffered (de-chunked) until the upstream closes and answered as an ordinary response.</li>
 * </ul>
 */
final class ProxyStream implements Tunnel.Sink {

    private static final Logger LOG = Logger.get(ProxyStream.class);
    private static final long MAX_PENDING_TO_CLIENT = 64L * 1024 * 1024;

    private enum Phase { HEAD, BUFFER, PIPE, DONE }

    private final Tunnel tunnel;
    private final int streamId;
    private final String method;
    private final boolean upgrade;
    private final CompletableFuture<HttpResponse> result;
    private final int maxBuffered;

    private Phase phase = Phase.HEAD;
    private final ByteArrayOutputStream buf = new ByteArrayOutputStream(1024);
    private int status;
    private Headers headers;
    private PipeStage stage;

    // buffered (non-2xx) responses
    private long contentLength = -1;
    private boolean chunked;
    private final ByteArrayOutputStream body = new ByteArrayOutputStream();
    private long chunkRemaining; // bytes left in the chunk being copied (chunked bodies)
    private ByteArrayOutputStream chunkLine = new ByteArrayOutputStream(32);
    private int chunkState; // 0 = size line, 1 = data, 2 = CRLF after data, 3 = trailers/done

    ProxyStream(Tunnel tunnel, int streamId, String method, boolean upgrade, CompletableFuture<HttpResponse> result, int maxBuffered) {
        this.tunnel = tunnel;
        this.streamId = streamId;
        this.method = method;
        this.upgrade = upgrade;
        this.result = result;
        this.maxBuffered = maxBuffered;
    }

    @Override
    public synchronized void onData(ByteBuffer data) {
        switch (phase) {
            case HEAD -> head(data);
            case BUFFER -> buffer(data);
            case PIPE -> stage.deliver(copy(data));
            case DONE -> {
            }
        }
    }

    @Override
    public synchronized void onClose(String reason) {
        switch (phase) {
            case HEAD -> fail(502, "Upstream " + (reason == null || reason.isEmpty() ? "closed before sending a response" : "closed: " + reason) + "\n");
            case BUFFER -> finishBuffered();
            case PIPE -> stage.upstreamClosed();
            case DONE -> {
            }
        }
        phase = Phase.DONE;
    }

    private void fail(int code, String message) {
        phase = Phase.DONE;
        result.complete(HttpResponse.status(code).text(message));
    }

    private void head(ByteBuffer data) {
        byte[] bytes = new byte[data.remaining()];
        data.get(bytes);
        buf.write(bytes, 0, bytes.length);
        byte[] all = buf.toByteArray();
        int end = indexOf(all, "\r\n\r\n".getBytes(StandardCharsets.ISO_8859_1));
        if (end < 0) {
            if (all.length > 64 * 1024) {
                tunnel.close(streamId);
                fail(502, "Upstream response head too large\n");
            }
            return;
        }
        String headText = new String(all, 0, end, StandardCharsets.ISO_8859_1);
        String[] lines = headText.split("\r\n", -1);
        String[] statusLine = lines[0].split(" ", 3);
        if (statusLine.length < 2 || !statusLine[0].startsWith("HTTP/")) {
            tunnel.close(streamId);
            fail(502, "Upstream sent a malformed response\n");
            return;
        }
        try {
            status = Integer.parseInt(statusLine[1]);
        } catch (NumberFormatException e) {
            tunnel.close(streamId);
            fail(502, "Upstream sent a malformed status\n");
            return;
        }
        headers = new Headers();
        for (int i = 1; i < lines.length; i++) {
            int colon = lines[i].indexOf(':');
            if (colon > 0) {
                headers.add(lines[i].substring(0, colon).trim(), lines[i].substring(colon + 1).trim());
            }
        }
        ByteBuffer rest = ByteBuffer.wrap(all, end + 4, all.length - end - 4);
        boolean switching = upgrade && status == 101;
        if (switching || (status >= 200 && status < 300 && !method.equals("HEAD"))) {
            phase = Phase.PIPE;
            stage = new PipeStage();
            if (rest.hasRemaining()) {
                stage.deliver(copy(rest));
            }
            HttpResponse response = HttpResponse.status(status);
            copyHeaders(response, switching);
            result.complete(switching ? response.upgrade(() -> stage) : response.stream(() -> stage));
            return;
        }
        phase = Phase.BUFFER;
        String cl = headers.get("content-length");
        chunked = headers.hasToken("transfer-encoding", "chunked");
        if (cl != null && !chunked) {
            try {
                contentLength = Long.parseLong(cl.trim());
            } catch (NumberFormatException ignored) {
                contentLength = -1;
            }
        }
        if (method.equals("HEAD") || status == 204 || status == 304 || status < 200) {
            contentLength = 0;
        }
        if (contentLength == 0) {
            finishBuffered();
            return;
        }
        if (rest.hasRemaining()) {
            buffer(rest);
        }
    }

    private void copyHeaders(HttpResponse response, boolean switching) {
        for (Map.Entry<String, String> e : headers.entries()) {
            String lower = e.getKey().toLowerCase(Locale.ROOT);
            if (lower.equals("content-length") || lower.equals("date") || lower.equals("keep-alive")) {
                continue;
            }
            if (!switching && (lower.equals("connection") || lower.equals("transfer-encoding") && !e.getValue().equalsIgnoreCase("chunked"))) {
                continue;
            }
            try {
                response.header(e.getKey(), e.getValue());
            } catch (IllegalArgumentException bad) {
                LOG.debug("dropping upstream header {}", e.getKey());
            }
        }
    }

    private void buffer(ByteBuffer data) {
        if (body.size() + data.remaining() > maxBuffered) {
            tunnel.close(streamId);
            fail(502, "Upstream response too large to buffer\n");
            return;
        }
        if (chunked) {
            dechunk(data);
            return;
        }
        byte[] bytes = new byte[data.remaining()];
        data.get(bytes);
        body.write(bytes, 0, bytes.length);
        if (contentLength >= 0 && body.size() >= contentLength) {
            finishBuffered();
        }
    }

    private void dechunk(ByteBuffer in) {
        while (in.hasRemaining() && chunkState != 3) {
            switch (chunkState) {
                case 0 -> {
                    byte b = in.get();
                    chunkLine.write(b);
                    if (b == '\n') {
                        String line = chunkLine.toString(StandardCharsets.ISO_8859_1).trim();
                        chunkLine = new ByteArrayOutputStream(32);
                        int semi = line.indexOf(';');
                        String hex = semi < 0 ? line : line.substring(0, semi);
                        if (hex.isEmpty()) {
                            continue; // stray CRLF
                        }
                        try {
                            chunkRemaining = Long.parseLong(hex.trim(), 16);
                        } catch (NumberFormatException e) {
                            chunkState = 3;
                            continue;
                        }
                        chunkState = chunkRemaining == 0 ? 3 : 1;
                    }
                }
                case 1 -> {
                    int n = (int) Math.min(chunkRemaining, in.remaining());
                    byte[] bytes = new byte[n];
                    in.get(bytes);
                    body.write(bytes, 0, n);
                    chunkRemaining -= n;
                    if (chunkRemaining == 0) {
                        chunkState = 2;
                    }
                }
                case 2 -> {
                    byte b = in.get();
                    if (b == '\n') {
                        chunkState = 0;
                    }
                }
                default -> {
                }
            }
        }
        if (chunkState == 3) {
            finishBuffered();
        }
    }

    private void finishBuffered() {
        if (phase == Phase.DONE) {
            return;
        }
        phase = Phase.DONE;
        tunnel.close(streamId);
        HttpResponse response = HttpResponse.status(status);
        copyHeaders(response, false);
        response.headers().remove("transfer-encoding");
        response.body(body.toByteArray());
        result.complete(response);
    }

    private static ByteBuffer copy(ByteBuffer data) {
        return ByteBuffer.allocate(data.remaining()).put(data).flip();
    }

    private static int indexOf(byte[] hay, byte[] needle) {
        outer:
        for (int i = 0; i <= hay.length - needle.length; i++) {
            for (int j = 0; j < needle.length; j++) {
                if (hay[i + j] != needle[j]) {
                    continue outer;
                }
            }
            return i;
        }
        return -1;
    }

    /**
     * The client connection after the takeover: upstream bytes (queued until the stage is active) are written through the
     * pipeline (so TLS encrypts them), client bytes go to the tunnel, with backpressure towards the client while the tunnel
     * WebSocket is not writable.
     */
    final class PipeStage implements Stage {
        private Context ctx;
        private final ArrayDeque<ByteBuffer> pending = new ArrayDeque<>();
        private boolean upstreamClosed;
        private boolean closed;

        @Override
        public synchronized void onActive(Context ctx) throws Exception {
            this.ctx = ctx;
            while (!pending.isEmpty()) {
                ctx.write(pending.poll());
            }
            if (upstreamClosed) {
                ctx.closeAfterFlush();
            }
        }

        @Override
        public void onRead(Context ctx, Object msg) {
            ByteBuffer data = (ByteBuffer) msg;
            tunnel.send(streamId, data);
            if (!tunnel.webSocket().isWritable()) {
                Connection c = ctx.connection();
                c.setAutoRead(false);
                tunnel.webSocket().whenWritable().whenComplete((v, e) -> {
                    if (e == null) {
                        c.setAutoRead(true);
                    } else {
                        c.close();
                    }
                });
            }
        }

        @Override
        public void onClose(Context ctx, Throwable cause) {
            closed = true;
            tunnel.close(streamId);
        }

        /** Called on the tunnel's loop. */
        synchronized void deliver(ByteBuffer data) {
            if (closed) {
                return;
            }
            if (ctx == null) {
                pending.add(data);
                return;
            }
            Context c = ctx;
            c.connection().eventLoop().execute(() -> {
                try {
                    if (c.connection().pendingBytes() > MAX_PENDING_TO_CLIENT) {
                        c.connection().close(); // the client is not reading; do not buffer without bound
                        return;
                    }
                    c.write(data);
                } catch (Exception e) {
                    c.connection().close();
                }
            });
        }

        synchronized void upstreamClosed() {
            upstreamClosed = true;
            if (ctx != null && !closed) {
                Context c = ctx;
                c.connection().eventLoop().execute(() -> {
                    try {
                        c.closeAfterFlush();
                    } catch (Exception e) {
                        c.connection().close();
                    }
                });
            }
        }
    }
}
