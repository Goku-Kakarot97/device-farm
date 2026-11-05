package com.mservice.farmstream.relay;

import com.mservice.farmstream.util.Config;
import com.mservice.farmstream.util.Pem;
import io.hydrangea.EventLoopGroup;
import io.hydrangea.websocket.WebSocket;
import io.hydrangea.websocket.WebSocketClient;
import io.hydrangea.websocket.WebSocketConfig;
import io.hydrangea.websocket.WebSocketListener;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * stdio <-> WebSocket bridge for the relay's {@code /t/} raw TCP routes, so plain TCP clients (ssh, adb, nc) travel through it:
 * <pre>
 *   ssh -o ProxyCommand='java -jar farm-stream.jar wspipe --insecure wss://<relay-host>/t/farm1/%h:%p' user@10.0.0.5
 * </pre>
 * {@code --insecure} (or RELAY_INSECURE=true) skips TLS certificate verification. Relay-side errors (no agent, target
 * unreachable) arrive as the WebSocket close reason and are printed to stderr.
 */
public final class WsPipe {

    private WsPipe() {
    }

    public static void run(List<String> args) throws Exception {
        boolean insecure = args.contains("--insecure") || Config.getBool("RELAY_INSECURE", false);
        String url = args.stream().filter(a -> !a.startsWith("--")).findFirst().orElse(null);
        if (url == null) {
            System.err.println("usage: wspipe [--insecure] wss://<relay-host>/t/<agent-id>/<host:port>");
            System.exit(2);
        }
        OutputStream out = System.out;
        WebSocketConfig config = WebSocketConfig.defaults().withMaxMessageBytes(16 * 1024 * 1024).withHandshakeTimeout(Duration.ofSeconds(15));
        if (insecure) {
            config = config.withSslContext(Pem.trustAll());
        }
        try (EventLoopGroup group = new EventLoopGroup(1)) {
            WebSocket ws;
            try {
                ws = WebSocketClient.connect(group, url, new WebSocketListener() {
                    @Override
                    public void onBinary(WebSocket ws, ByteBuffer data) throws IOException {
                        byte[] bytes = new byte[data.remaining()];
                        data.get(bytes);
                        out.write(bytes);
                        out.flush();
                    }

                    @Override
                    public void onError(WebSocket ws, Throwable error) {
                        System.err.println("[wspipe] " + error.getMessage());
                    }

                    @Override
                    public void onClose(WebSocket ws, int code, String reason) {
                        // 1011 carries relay-side errors (no agent, target unreachable) in the reason
                        if (code != 1000 && code != 1005 && reason != null && !reason.isEmpty()) {
                            System.err.println("[wspipe] relay closed (" + code + "): " + reason);
                        }
                        System.out.flush();
                        System.exit(code == 1000 || code == 1005 ? 0 : 1);
                    }
                }, config).get(20, TimeUnit.SECONDS);
            } catch (Exception e) {
                Throwable cause = e.getCause() != null ? e.getCause() : e;
                System.err.println("[wspipe] " + cause.getMessage());
                System.exit(1);
                return;
            }
            InputStream in = System.in;
            byte[] buf = new byte[64 * 1024];
            int n;
            while ((n = in.read(buf)) > 0) {
                ws.whenWritable().get();
                ws.sendBinary(ByteBuffer.wrap(buf, 0, n)).get();
            }
            // stdin ended: the target usually answers and closes first (the relay then closes the WebSocket, which exits
            // through onClose); give it a moment, then close ourselves
            Thread.sleep(2000);
            ws.close(1000, "").get(5, TimeUnit.SECONDS);
            Thread.sleep(200);
            System.exit(0);
        }
    }
}
