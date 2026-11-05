package com.mservice.farmstream;

import com.mservice.farmstream.util.Pem;
import io.hydrangea.tls.TlsStage;
import org.junit.jupiter.api.Test;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import java.nio.ByteBuffer;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** The self-signed ssl/server.crt is issued for another name; a trust-all client with "HTTPS" endpoint identification must still handshake. */
class TrustAllTest {

    @Test
    void handshakeIgnoresHostName() throws Exception {
        SSLContext serverCtx = Pem.serverContext(Path.of("ssl/server.crt"), Path.of("ssl/server.key"));
        SSLEngine server = serverCtx.createSSLEngine();
        server.setUseClientMode(false);
        // same engine setup as hydrangea WebSocketClient -> TlsStage.client(): endpoint identification "HTTPS"
        SSLEngine client = TlsStage.client(Pem.trustAll(), "name-that-is-not-in-the-cert.example", 443).engine();

        ByteBuffer c2s = ByteBuffer.allocate(64 * 1024), s2c = ByteBuffer.allocate(64 * 1024);
        ByteBuffer empty = ByteBuffer.allocate(0), sink = ByteBuffer.allocate(64 * 1024);
        client.beginHandshake();
        server.beginHandshake();
        for (int i = 0; i < 200 && (client.getHandshakeStatus() != SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING
                || server.getHandshakeStatus() != SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING); i++) {
            step(client, c2s, s2c, empty, sink);
            step(server, s2c, c2s, empty, sink);
        }
        assertEquals(SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING, client.getHandshakeStatus());
        assertEquals(SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING, server.getHandshakeStatus());
    }

    private static void step(SSLEngine e, ByteBuffer out, ByteBuffer in, ByteBuffer empty, ByteBuffer sink) throws Exception {
        switch (e.getHandshakeStatus()) {
            case NEED_WRAP -> e.wrap(empty, out);
            case NEED_UNWRAP -> {
                in.flip();
                e.unwrap(in, sink);
                in.compact();
            }
            case NEED_TASK -> {
                Runnable r;
                while ((r = e.getDelegatedTask()) != null) r.run();
            }
            default -> { }
        }
    }
}
