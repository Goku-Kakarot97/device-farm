package com.mservice.farmstream.device;

import com.mservice.farmstream.adb.AdbClient;
import com.mservice.farmstream.util.Config;
import com.mservice.farmstream.util.Pem;
import io.hydrangea.EventLoopGroup;
import io.hydrangea.Options;
import io.hydrangea.Server;
import io.hydrangea.http.HttpConfig;
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
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.function.Supplier;

/**
 * The device streaming server (the ws-scrcpy fork that used to be streaming-device): HTTPS + WebSocket on one port
 * (LISTEN_PORT, 11000). WebSocket actions on {@code /?action=...}: devices-list, proxy-adb, proxy-ws, record-status,
 * goog-device-list; plus the {@code /proxy-adb/<udid>/<remote>/<path>} shortcut. The JSON API lives in {@link ApiRoutes}.
 * TLS uses CERT_PATH/KEY_PATH or {@code ssl/server.crt|key} (NO_TLS=true for plain HTTP).
 */
public final class DeviceServer {

    private static final Logger LOG = Logger.get(DeviceServer.class);
    private static final String TAG = "WebSocket Server";

    private final EventLoopGroup group;
    private AdbClient adb;
    private DeviceRepository repo;
    private DeviceRegistry registry;
    private Server server;

    public DeviceServer(EventLoopGroup group) {
        this.group = group;
    }

    public void start() throws Exception {
        int port = Config.getInt("LISTEN_PORT", 11000);
        adb = new AdbClient();
        repo = DeviceRepository.connect(group);
        RecordingStatusService recording = new RecordingStatusService();
        DeviceStatusService status = new DeviceStatusService(repo, recording);
        status.init(); // DB table + stored records before accepting connections
        SyncService sync = new SyncService();
        ConnectPreferenceService preference = new ConnectPreferenceService();
        KeepAwakeService keepAwake = new KeepAwakeService(adb);
        registry = new DeviceRegistry(adb, status);
        DeviceListService deviceList = new DeviceListService(adb, registry, status, recording, preference);
        StreamProxy.Services services = new StreamProxy.Services(group, adb, recording, sync, status);
        registry.start();

        WebSocketConfig ws = WebSocketConfig.defaults()
                .allowAnyOrigin()
                .withMaxMessageBytes(50 * 1024 * 1024)
                .withPingInterval(Duration.ofSeconds(30));
        HttpRouter router = new HttpRouter().tracing(false);
        new ApiRoutes(adb, registry, deviceList, status, sync, keepAwake, preference, services).register(router);
        router.get("/", r -> {
            if (!r.request().headers().hasToken("upgrade", "websocket")) {
                return HttpResponse.status(404).text("farm-stream device server: WebSocket actions on /?action=... and JSON API under /api/\n");
            }
            return WebSocketUpgrade.accept(r.request(), ws, dispatch(r, services, deviceList, recording));
        });
        router.get("/proxy-adb/{udid}/{remote}/{path...}", r -> WebSocketUpgrade.accept(r.request(), ws,
                () -> proxyAdb(r, services, r.pathVariable("udid"), r.pathVariable("remote"), "/" + r.pathVariable("path"))));
        router.get("/proxy-adb/{udid}/{remote}", r -> WebSocketUpgrade.accept(r.request(), ws,
                () -> proxyAdb(r, services, r.pathVariable("udid"), r.pathVariable("remote"), "/")));
        router.notFound(r -> ApiRoutes.cors(HttpResponse.status(404).text("Not Found")));

        SSLContext tls = loadTls();
        HttpConfig http = new HttpConfig(64 * 1024, 256 * 1024 * 1024)
                .withBodyTimeout(Duration.ofMinutes(10))
                .withKeepAliveTimeout(Duration.ofSeconds(120));
        server = HttpServer.bind(group, new InetSocketAddress(port), Options.defaults(), tls, http, router);
        LOG.info("device server listening on {}://0.0.0.0:{}/", tls != null ? "https" : "http", port);
    }

    public void stop() {
        if (registry != null) {
            registry.stop();
        }
        if (server != null) {
            server.close();
        }
        if (repo != null) {
            repo.close();
        }
    }

    private static SSLContext loadTls() throws Exception {
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

    /** Picks the listener for {@code /?action=...}; unknown or invalid requests are accepted and closed with 4002/4003 like ws-scrcpy. */
    private Supplier<WebSocketListener> dispatch(RouteRequest r, StreamProxy.Services services, DeviceListService deviceList,
                                                 RecordingStatusService recording) {
        String action = r.param("action", "");
        return switch (action) {
            case "devices-list" -> deviceList::listener;
            case "record-status" -> () -> new RecordingStatusSocket(recording);
            case "goog-device-list" -> () -> new DeviceTrackerSocket(registry);
            case "proxy-adb" -> () -> proxyAdb(r, services, r.param("udid"), r.param("remote"), r.param("path"));
            case "proxy-ws" -> () -> {
                String wsUrl = r.param("ws");
                if (wsUrl == null || wsUrl.isEmpty()) {
                    return closing(4003, "[WebsocketProxy] Invalid value \"" + wsUrl + "\" for \"ws\" parameter");
                }
                String recordId = RecordingStore.normalizeId(r.param("record") != null ? orTrue(r.param("record")) : null);
                String replayId = RecordingStore.normalizeId(r.param("replay"));
                if (r.param("replay") != null && replayId == null) {
                    return closing(4003, "[WebsocketProxy] Invalid value \"" + r.param("replay") + "\" for \"replay\" parameter");
                }
                return StreamProxy.direct(services, wsUrl, recordId, replayId, r.param("session"));
            };
            default -> () -> closing(4002, "[" + TAG + "] Unsupported request");
        };
    }

    private static String orTrue(String v) {
        return v.isEmpty() ? "true" : v;
    }

    private WebSocketListener proxyAdb(RouteRequest r, StreamProxy.Services services, String udid, String remote, String path) {
        if (remote == null || remote.isEmpty()) {
            return closing(4003, "[WebsocketProxyOverAdb] Invalid value \"" + remote + "\" for \"remote\" parameter");
        }
        if (udid == null || udid.isEmpty()) {
            return closing(4003, "[WebsocketProxyOverAdb] Invalid value \"" + udid + "\" for \"udid\" parameter");
        }
        String recordId = RecordingStore.normalizeId(r.param("record") != null ? orTrue(r.param("record")) : null);
        String replayId = RecordingStore.normalizeId(r.param("replay"));
        if (r.param("replay") != null && replayId == null) {
            return closing(4003, "[WebsocketProxyOverAdb] Invalid value \"" + r.param("replay") + "\" for \"replay\" parameter");
        }
        String user = r.param("user");
        return StreamProxy.overAdb(services, udid, remote, path, recordId, replayId, user == null || user.isEmpty() ? null : user);
    }

    private static WebSocketListener closing(int code, String reason) {
        return new WebSocketListener() {
            @Override
            public void onOpen(WebSocket ws) {
                ws.close(code, reason.length() > 120 ? reason.substring(0, 120) : reason);
            }
        };
    }
}
