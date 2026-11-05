package com.mservice.farmstream.relay;

import com.mservice.farmstream.util.Config;
import com.mservice.farmstream.util.Json;
import com.mservice.farmstream.util.Pem;
import io.hydrangea.Backoff;
import io.hydrangea.Client;
import io.hydrangea.Connection;
import io.hydrangea.EventLoopGroup;
import io.hydrangea.Options;
import io.hydrangea.log.Logger;
import io.hydrangea.pipeline.Context;
import io.hydrangea.pipeline.Pipeline;
import io.hydrangea.pipeline.Stage;
import io.hydrangea.tls.TlsStage;
import io.hydrangea.websocket.WebSocket;
import io.hydrangea.websocket.WebSocketClient;
import io.hydrangea.websocket.WebSocketConfig;
import io.hydrangea.websocket.WebSocketListener;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * relay-agent: runs on a machine inside the device-farm LAN. Dials out to the relay-server, keeps a persistent tunnel
 * (reconnecting with exponential backoff) and opens local TCP/TLS connections to farm hosts on demand.
 * <p>
 * Env: RELAY_URL (required, {@code wss://<relay>/tunnel}), AGENT_ID (hostname), TUNNEL_TOKEN, ALLOW (CIDRs of allowed
 * targets, default {@code 127.0.0.1/32}, {@code *} disables the check), DEFAULT_PORT (11000),
 * RELAY_INSECURE=true to skip verification of the relay's self-signed certificate.
 */
public final class TunnelAgent {

    private static final Logger LOG = Logger.get(TunnelAgent.class);
    private static final Pattern TARGET = Pattern.compile("^([A-Za-z0-9.\\-]+?)(?::(\\d{1,5}))?$");

    private final EventLoopGroup group;
    private final String relayUrl;
    private final String agentId;
    private final String token;
    private final boolean insecure;
    private final int defaultPort;
    private final List<String> allow;
    private final Backoff backoff = Backoff.defaults().withJitter(0);
    private int attempt;
    private volatile boolean stopped;

    public TunnelAgent(EventLoopGroup group) {
        this.group = group;
        this.relayUrl = Config.get("RELAY_URL", "");
        String id = Config.get("AGENT_ID");
        if (id == null) {
            try {
                id = InetAddress.getLocalHost().getHostName();
            } catch (Exception e) {
                id = "farm";
            }
        }
        this.agentId = id;
        this.token = Config.get("TUNNEL_TOKEN", "");
        this.insecure = Config.getBool("RELAY_INSECURE", false);
        this.defaultPort = Config.getInt("DEFAULT_PORT", 11000);
        List<String> cidrs = new ArrayList<>();
        for (String s : Config.get("ALLOW", "127.0.0.1/32").split(",")) {
            if (!s.isBlank()) {
                cidrs.add(s.trim());
            }
        }
        this.allow = cidrs;
    }

    public boolean configured() {
        return !relayUrl.isEmpty();
    }

    public String agentId() {
        return agentId;
    }

    public void start() {
        if (relayUrl.isEmpty()) {
            throw new IllegalStateException("RELAY_URL is required, e.g. RELAY_URL=wss://relay.example.com/tunnel");
        }
        connect();
    }

    public void stop() {
        stopped = true;
    }

    private void connect() {
        if (stopped) {
            return;
        }
        URI base = URI.create(relayUrl);
        StringBuilder url = new StringBuilder(relayUrl);
        url.append(base.getRawQuery() == null ? '?' : '&').append("id=").append(URLEncoder.encode(agentId, StandardCharsets.UTF_8));
        if (!token.isEmpty()) {
            url.append("&token=").append(URLEncoder.encode(token, StandardCharsets.UTF_8));
        }
        LOG.info("connecting to {} as \"{}\"...", base.getScheme() + "://" + base.getAuthority() + base.getPath(), agentId);
        WebSocketConfig config = WebSocketConfig.defaults()
                .withMaxMessageBytes(16 * 1024 * 1024)
                .withPingInterval(Duration.ofSeconds(20))
                .withHandshakeTimeout(Duration.ofSeconds(15));
        if (insecure) {
            config = config.withSslContext(Pem.trustAll());
        }
        Session session = new Session();
        WebSocketClient.connect(group, url.toString(), session, config).whenComplete((ws, err) -> {
            if (err != null) {
                Throwable cause = err instanceof CompletionException && err.getCause() != null ? err.getCause() : err;
                LOG.error("ws error: {}", cause.toString());
                scheduleReconnect();
            }
        });
    }

    private void scheduleReconnect() {
        if (stopped) {
            return;
        }
        Duration delay = backoff.delayFor(++attempt);
        LOG.info("disconnected, retrying in {}ms", delay.toMillis());
        group.next().schedule(this::connect, delay.toMillis(), TimeUnit.MILLISECONDS);
    }

    // ---- one tunnel connection ----

    private final class Session implements WebSocketListener {
        private final Map<Integer, Target> streams = new ConcurrentHashMap<>();
        private WebSocket ws;
        private boolean reconnectScheduled;

        @Override
        public void onOpen(WebSocket ws) {
            this.ws = ws;
            attempt = 0;
            LOG.info("tunnel established (allow: {}, default port {})", String.join(",", allow), defaultPort);
        }

        @Override
        public void onBinary(WebSocket ws, ByteBuffer data) {
            Frame frame = Frame.decode(data);
            if (frame == null) {
                return;
            }
            int streamId = frame.streamId();
            switch (frame.type()) {
                case Frame.OPEN -> open(streamId, frame.payloadText());
                case Frame.DATA -> {
                    Target t = streams.get(streamId);
                    if (t != null) {
                        t.write(ByteBuffer.allocate(frame.payload().remaining()).put(frame.payload()).flip());
                    }
                }
                case Frame.CLOSE -> {
                    Target t = streams.remove(streamId);
                    if (t != null) {
                        t.end();
                    }
                }
                default -> {
                }
            }
        }

        @Override
        public void onError(WebSocket ws, Throwable error) {
            LOG.error("ws error: {}", error.toString());
        }

        @Override
        public void onClose(WebSocket ws, int code, String reason) {
            for (Target t : streams.values()) {
                t.destroy();
            }
            streams.clear();
            if (!reconnectScheduled) {
                reconnectScheduled = true;
                scheduleReconnect();
            }
        }

        private void open(int streamId, String payload) {
            String targetText;
            boolean tls;
            try {
                Map<String, Object> req = Json.asObject(Json.parse(payload));
                targetText = Json.str(req, "target");
                tls = Json.bool(req, "tls");
            } catch (RuntimeException e) {
                fail(streamId, "invalid OPEN payload");
                return;
            }
            Matcher m = TARGET.matcher(targetText == null ? "" : targetText);
            if (!m.matches()) {
                fail(streamId, "invalid target \"" + targetText + "\"");
                return;
            }
            String host = m.group(1);
            int port = m.group(2) != null ? Integer.parseInt(m.group(2)) : defaultPort;
            if (!isAllowed(host)) {
                fail(streamId, "target " + host + " not in allowlist (" + String.join(",", allow) + ")");
                return;
            }
            Target target = new Target(streamId);
            streams.put(streamId, target);
            Stage[] stages = tls
                    ? new Stage[] {TlsStage.of(Pem.insecureClientEngine(host, port)), target}
                    : new Stage[] {target};
            Client.connect(group, new InetSocketAddress(host, port), Options.defaults().withConnectTimeout(Duration.ofSeconds(8)), Pipeline.of(stages))
                    .whenComplete((c, err) -> {
                        if (err != null) {
                            Throwable cause = err instanceof CompletionException && err.getCause() != null ? err.getCause() : err;
                            streams.remove(streamId);
                            fail(streamId, cause.getMessage() == null ? cause.toString() : cause.getMessage());
                        }
                    });
        }

        private void fail(int streamId, String message) {
            LOG.warn("stream {} failed: {}", streamId, message);
            if (ws != null && ws.isOpen()) {
                ws.sendBinary(Frame.encode(Frame.OPEN_FAIL, streamId, message));
            }
        }

        /** The TCP/TLS connection to one farm host; last stage of its pipeline, so writes go through TLS when present. */
        private final class Target implements Stage {
            private final int streamId;
            private Context ctx;
            private boolean active;
            private boolean ended;
            private final ArrayDeque<ByteBuffer> pending = new ArrayDeque<>(); // bytes received before the (TLS) connection is active

            Target(int streamId) {
                this.streamId = streamId;
            }

            @Override
            public synchronized void onActive(Context ctx) throws Exception {
                this.ctx = ctx;
                active = true;
                if (!streams.containsKey(streamId)) { // closed by the relay while we were dialing
                    ctx.connection().close();
                    return;
                }
                ws.sendBinary(Frame.encode(Frame.OPEN_OK, streamId));
                while (!pending.isEmpty()) {
                    ctx.write(pending.poll());
                }
                if (ended) {
                    ctx.closeAfterFlush();
                }
            }

            @Override
            public void onRead(Context ctx, Object msg) {
                ByteBuffer data = (ByteBuffer) msg;
                if (!ws.isOpen()) {
                    ctx.connection().close();
                    return;
                }
                ws.sendBinary(Frame.encode(Frame.DATA, streamId, data));
                if (!ws.isWritable()) {
                    Connection c = ctx.connection();
                    c.setAutoRead(false);
                    ws.whenWritable().whenComplete((v, e) -> {
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
                boolean wasActive;
                synchronized (this) {
                    wasActive = active;
                    active = false;
                }
                if (streams.remove(streamId) != null) {
                    if (wasActive) {
                        if (ws.isOpen()) {
                            ws.sendBinary(Frame.encode(Frame.CLOSE, streamId));
                        }
                    } else {
                        fail(streamId, cause == null ? "connection closed before it was established" : String.valueOf(cause.getMessage()));
                    }
                }
            }

            /** From the tunnel loop. */
            synchronized void write(ByteBuffer data) {
                if (!active) {
                    pending.add(data);
                    return;
                }
                Context c = ctx;
                c.connection().eventLoop().execute(() -> {
                    try {
                        c.write(data);
                    } catch (Exception e) {
                        c.connection().close();
                    }
                });
            }

            synchronized void end() {
                ended = true;
                if (active) {
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

            synchronized void destroy() {
                if (ctx != null) {
                    ctx.connection().close();
                }
            }
        }
    }

    // ---- allowlist ----

    boolean isAllowed(String host) {
        if (allow.contains("*")) {
            return true;
        }
        long ip = ipToInt(host);
        if (ip < 0) {
            return false; // only IP-literal targets unless ALLOW=*
        }
        for (String cidr : allow) {
            String[] parts = cidr.split("/", 2);
            long base = ipToInt(parts[0]);
            if (base < 0) {
                continue;
            }
            int bits;
            try {
                bits = parts.length == 1 ? 32 : Integer.parseInt(parts[1]);
            } catch (NumberFormatException e) {
                continue;
            }
            if (bits < 0 || bits > 32) {
                continue;
            }
            long mask = bits == 0 ? 0 : (0xffffffffL << (32 - bits)) & 0xffffffffL;
            if ((ip & mask) == (base & mask)) {
                return true;
            }
        }
        return false;
    }

    static long ipToInt(String ip) {
        String[] parts = ip.split("\\.", -1);
        if (parts.length != 4) {
            return -1;
        }
        long value = 0;
        for (String p : parts) {
            int n;
            try {
                n = Integer.parseInt(p);
            } catch (NumberFormatException e) {
                return -1;
            }
            if (n < 0 || n > 255) {
                return -1;
            }
            value = (value << 8) | n;
        }
        return value;
    }
}
