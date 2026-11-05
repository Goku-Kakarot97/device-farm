package com.mservice.farmstream.relay;

import com.mservice.farmstream.util.Config;
import com.mservice.farmstream.util.Json;
import com.mservice.farmstream.util.Pem;
import io.hydrangea.EventLoopGroup;
import io.hydrangea.Options;
import io.hydrangea.Server;
import io.hydrangea.http.Headers;
import io.hydrangea.http.HttpConfig;
import io.hydrangea.http.HttpRequest;
import io.hydrangea.http.HttpResponse;
import io.hydrangea.http.HttpRouter;
import io.hydrangea.http.HttpServer;
import io.hydrangea.http.RouteRequest;
import io.hydrangea.log.Logger;
import io.hydrangea.websocket.WebSocket;
import io.hydrangea.websocket.WebSocketConfig;
import io.hydrangea.websocket.WebSocketListener;
import io.hydrangea.websocket.WebSocketUpgrade;

import javax.net.ssl.SSLContext;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;

/**
 * relay-server: runs in k8s (reachable over VPN).
 * <ul>
 *   <li>Agents inside the device-farm LAN dial out to {@code GET /tunnel?id=<agentId>&token=<TUNNEL_TOKEN>} and keep a
 *       persistent WebSocket open (see {@link TunnelAgent}).</li>
 *   <li>Clients hit {@code /d/<agentId>/<host[:port]>/<original path>} (TLS upstream) or {@code /p/...} (plain TCP) for both
 *       plain HTTP and WebSocket upgrades: the request head is rewritten and replayed on a logical stream over the agent's
 *       tunnel, and the raw bytes are piped both ways.</li>
 *   <li>{@code /t/<agentId>/<host[:port]>} is a raw TCP bridge for non-HTTP targets (ssh, adb): the relay answers the
 *       WebSocket handshake itself and maps binary messages to raw bytes.</li>
 * </ul>
 * Env: PORT (18000), TUNNEL_TOKEN, OPEN_TIMEOUT_MS (10000), CERT_PATH + KEY_PATH (default: bundled ssl/server.crt|key next
 * to the working directory or its parent), NO_TLS=true to serve plain HTTP (TLS at an ingress).
 */
public final class RelayServer {

    private static final Logger LOG = Logger.get(RelayServer.class);
    private static final int MAX_BUFFERED_RESPONSE = 64 * 1024 * 1024;

    private final Map<String, Tunnel> tunnels = new ConcurrentHashMap<>();
    private final String tunnelToken;
    private final long openTimeoutMs;

    private RelayServer(String tunnelToken, long openTimeoutMs) {
        this.tunnelToken = tunnelToken;
        this.openTimeoutMs = openTimeoutMs;
    }

    public static void run() throws Exception {
        int port = Config.getInt("PORT", 18000);
        String token = Config.get("TUNNEL_TOKEN", "");
        RelayServer relay = new RelayServer(token, Config.getInt("OPEN_TIMEOUT_MS", 10000));
        SSLContext tls = loadTls();
        EventLoopGroup group = new EventLoopGroup();
        HttpConfig http = new HttpConfig(64 * 1024, 256 * 1024 * 1024)
                .withBodyTimeout(Duration.ofMinutes(10))
                .withKeepAliveTimeout(Duration.ofSeconds(60));
        Server server = HttpServer.bind(group, new InetSocketAddress(port), Options.defaults(), tls, http, relay.router());
        LOG.info("listening on :{} ({})", port, tls != null ? "https/wss" : "plain http/ws - TLS at ingress");
        if (token.isEmpty()) {
            LOG.warn("WARNING: TUNNEL_TOKEN is not set - any agent can register");
        }
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            server.close();
            group.close();
        }));
        Thread.currentThread().join();
    }

    static SSLContext loadTls() throws Exception {
        if (Config.getBool("NO_TLS", false)) {
            return null;
        }
        String certPath = Config.get("CERT_PATH");
        String keyPath = Config.get("KEY_PATH");
        if (certPath == null || keyPath == null) {
            for (String dir : new String[] {"ssl", "../ssl"}) {
                Path c = Path.of(dir, "server.crt");
                Path k = Path.of(dir, "server.key");
                if (Files.exists(c) && Files.exists(k)) {
                    certPath = c.toString();
                    keyPath = k.toString();
                    break;
                }
            }
        }
        if (certPath == null || keyPath == null) {
            LOG.warn("no TLS certificate found (CERT_PATH/KEY_PATH or ssl/server.crt|key): serving plain HTTP");
            return null;
        }
        return Pem.serverContext(Path.of(certPath), Path.of(keyPath));
    }

    HttpRouter router() {
        WebSocketConfig tunnelWs = WebSocketConfig.defaults()
                .allowAnyOrigin()
                .withMaxMessageBytes(16 * 1024 * 1024)
                .withPingInterval(Duration.ofSeconds(20));
        WebSocketConfig rawWs = WebSocketConfig.defaults()
                .allowAnyOrigin()
                .withMaxMessageBytes(16 * 1024 * 1024)
                .withPingInterval(Duration.ofSeconds(20));
        HttpRouter router = new HttpRouter().tracing(false)
                .get("/healthz", r -> HttpResponse.ok("ok"))
                .get("/agents", r -> {
                    List<Object> list = new ArrayList<>();
                    for (Tunnel t : tunnels.values()) {
                        list.add(Json.obj("id", t.id, "connectedAt", t.connectedAt.toString(), "activeStreams", t.streamCount()));
                    }
                    return HttpResponse.status(200).header("Content-Type", "application/json").body(Json.stringify(list).getBytes(StandardCharsets.UTF_8));
                })
                .get("/tunnel", r -> {
                    String id = r.param("id");
                    String token = r.param("token", "");
                    if (id == null || id.isEmpty()) {
                        return HttpResponse.status(400).text("Missing \"id\"");
                    }
                    if (!tunnelToken.isEmpty() && !tunnelToken.equals(token)) {
                        LOG.warn("rejected tunnel \"{}\": bad token", id);
                        return HttpResponse.status(401).text("Unauthorized");
                    }
                    return WebSocketUpgrade.accept(r.request(), tunnelWs, () -> new TunnelListener(id));
                })
                .get("/t/{agent}/{target}", r -> {
                    ProxyRoute route = ProxyRoute.parse(r.request().target());
                    if (route == null) {
                        return HttpResponse.status(404).text("Not Found");
                    }
                    if (!r.request().headers().hasToken("upgrade", "websocket")) {
                        return HttpResponse.status(400).text("/t/ is a raw TCP bridge: connect with a WebSocket client\n");
                    }
                    return WebSocketUpgrade.accept(r.request(), rawWs, () -> new RawBridge(route));
                })
                .notFound(r -> HttpResponse.status(404).text(
                        "Not found. Use /d/<agent>/<host:port>/<path> (TLS upstream), /p/... (plain TCP) or /t/<agent>/<host:port> (raw TCP over WS)\n"));
        for (String method : new String[] {"GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"}) {
            router.routeAsync(method, "/{path...}", this::proxy);
        }
        return router;
    }

    // ---- agent registration ----

    private final class TunnelListener implements WebSocketListener {
        private final String id;
        private Tunnel tunnel;

        TunnelListener(String id) {
            this.id = id;
        }

        @Override
        public void onOpen(WebSocket ws) {
            tunnel = new Tunnel(id, ws, openTimeoutMs);
            Tunnel existing = tunnels.put(id, tunnel);
            if (existing != null) {
                existing.destroy("replaced by new tunnel connection");
            }
            LOG.info("agent \"{}\" connected ({} total)", id, tunnels.size());
        }

        @Override
        public void onBinary(WebSocket ws, ByteBuffer data) {
            tunnel.onFrame(data);
        }

        @Override
        public void onError(WebSocket ws, Throwable error) {
            LOG.warn("tunnel \"{}\" error: {}", id, String.valueOf(error));
        }

        @Override
        public void onClose(WebSocket ws, int code, String reason) {
            if (tunnel != null && tunnels.remove(id, tunnel)) {
                tunnel.destroy("tunnel closed");
            }
        }
    }

    // ---- /t/ raw TCP bridge ----

    private final class RawBridge implements WebSocketListener, Tunnel.Sink {
        private final ProxyRoute route;
        private WebSocket ws;
        private Tunnel tunnel;
        private int streamId = -1;
        private boolean closed;
        private final ArrayDeque<ByteBuffer> early = new ArrayDeque<>(); // client bytes that arrive while the agent dials

        RawBridge(ProxyRoute route) {
            this.route = route;
        }

        @Override
        public void onOpen(WebSocket ws) {
            this.ws = ws;
            tunnel = tunnels.get(route.agentId());
            if (tunnel == null) {
                ws.close(1011, "No agent \"" + route.agentId() + "\" connected");
                return;
            }
            tunnel.open(route.target(), route.tls()).whenComplete((id, err) -> ws.eventLoop().execute(() -> {
                if (err != null) {
                    ws.close(1011, truncate("Could not reach " + route.target() + ": " + err.getMessage()));
                    return;
                }
                if (closed) {
                    tunnel.close(id);
                    return;
                }
                streamId = id;
                tunnel.attach(id, this);
                while (!early.isEmpty()) {
                    tunnel.send(id, early.poll());
                }
            }));
        }

        @Override
        public void onBinary(WebSocket ws, ByteBuffer data) {
            if (streamId < 0) {
                early.add(data);
                return;
            }
            tunnel.send(streamId, data);
            if (!tunnel.webSocket().isWritable()) {
                ws.setAutoRead(false);
                tunnel.webSocket().whenWritable().whenComplete((v, e) -> ws.setAutoRead(true));
            }
        }

        @Override
        public void onClose(WebSocket ws, int code, String reason) {
            closed = true;
            if (streamId >= 0) {
                tunnel.close(streamId);
            }
        }

        @Override
        public void onData(ByteBuffer data) {
            ws.sendBinary(data);
        }

        @Override
        public void onClose(String reason) {
            streamId = -1;
            if (ws.isOpen()) {
                ws.close(1000, "");
            }
        }
    }

    // ---- /d/ /p/ /dk/ /pk/ HTTP and WebSocket proxying ----

    private CompletionStage<HttpResponse> proxy(RouteRequest r) {
        HttpRequest req = r.request();
        ProxyRoute route = ProxyRoute.parse(req.target());
        if (route == null) {
            return done(HttpResponse.status(404).text(
                    "Not found. Use /d/<agent>/<host:port>/<path> (TLS upstream), /p/... (plain TCP) or /t/<agent>/<host:port> (raw TCP over WS)\n"));
        }
        if (route.raw()) {
            return done(HttpResponse.status(400).text("/t/ is a raw TCP bridge: connect with a WebSocket client\n"));
        }
        Tunnel tunnel = tunnels.get(route.agentId());
        if (tunnel == null) {
            return done(HttpResponse.status(502).text("No agent \"" + route.agentId() + "\" connected\n"));
        }
        boolean upgrade = req.headers().hasToken("upgrade", "websocket");
        byte[] head = buildRequestHead(req, route, upgrade);
        CompletableFuture<HttpResponse> result = new CompletableFuture<>();
        tunnel.open(route.target(), route.tls()).whenComplete((streamId, err) -> {
            if (err != null) {
                result.complete(HttpResponse.status(502).text("Agent \"" + route.agentId() + "\" could not reach " + route.target() + ": " + err.getMessage() + "\n"));
                return;
            }
            ProxyStream stream = new ProxyStream(tunnel, streamId, req.method(), upgrade, result, MAX_BUFFERED_RESPONSE);
            tunnel.attach(streamId, stream);
            tunnel.send(streamId, ByteBuffer.wrap(head));
            if (req.body().length > 0) {
                tunnel.send(streamId, ByteBuffer.wrap(req.body()));
            }
        });
        return result;
    }

    /** Rebuilds the request head with a rewritten path/host so it can be replayed upstream (hop-by-hop headers dropped). */
    static byte[] buildRequestHead(HttpRequest req, ProxyRoute route, boolean upgrade) {
        StringBuilder sb = new StringBuilder(512);
        sb.append(req.method()).append(' ').append(route.rest()).append(" HTTP/1.1\r\n");
        Headers h = req.headers();
        boolean hasBody = req.body().length > 0 || req.method().equals("POST") || req.method().equals("PUT") || req.method().equals("PATCH");
        for (Map.Entry<String, String> e : h.entries()) {
            String name = e.getKey();
            String lower = name.toLowerCase(java.util.Locale.ROOT);
            if (lower.equals("host")) {
                sb.append("Host: ").append(route.target()).append("\r\n");
                continue;
            }
            // the body was de-chunked and buffered by the HTTP stage: it is sent with a Content-Length instead
            if (lower.equals("content-length") || lower.equals("transfer-encoding") || lower.equals("expect")) {
                continue;
            }
            // for plain HTTP force one-request-per-connection so raw piping stays simple
            if (!upgrade && (lower.equals("connection") || lower.equals("keep-alive") || lower.equals("proxy-connection"))) {
                continue;
            }
            sb.append(name).append(": ").append(e.getValue()).append("\r\n");
        }
        if (hasBody) {
            sb.append("Content-Length: ").append(req.body().length).append("\r\n");
        }
        if (!upgrade) {
            sb.append("Connection: close\r\n");
        }
        sb.append("\r\n");
        return sb.toString().getBytes(StandardCharsets.ISO_8859_1);
    }

    private static CompletionStage<HttpResponse> done(HttpResponse r) {
        return CompletableFuture.completedFuture(r);
    }

    private static String truncate(String reason) {
        byte[] b = reason.getBytes(StandardCharsets.UTF_8);
        return b.length <= 120 ? reason : new String(b, 0, 120, StandardCharsets.UTF_8).replaceAll("\\p{Cntrl}|\\uFFFD", "");
    }
}
