package com.mservice.farmstream.relay;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ProtocolTest {

    @Test
    void frameRoundTripMatchesNodeLayout() {
        ByteBuffer f = Frame.encode(Frame.OPEN, 0x01020304, "{\"target\":\"a:1\",\"tls\":true}");
        byte[] bytes = new byte[f.remaining()];
        f.duplicate().get(bytes);
        assertEquals(1, bytes[0]);
        assertEquals(1, bytes[1]);
        assertEquals(4, bytes[4]);
        Frame d = Frame.decode(f);
        assertEquals(Frame.OPEN, d.type());
        assertEquals(0x01020304, d.streamId());
        assertEquals("{\"target\":\"a:1\",\"tls\":true}", d.payloadText());
        assertNull(Frame.decode(ByteBuffer.wrap(new byte[3])));
        assertEquals(0, Frame.decode(Frame.encode(Frame.CLOSE, 7)).payload().remaining());
    }

    @Test
    void routes() {
        ProxyRoute d = ProxyRoute.parse("/d/farm1/10.0.0.36:11000/?action=proxy-adb&udid=x");
        assertEquals("farm1", d.agentId());
        assertEquals("10.0.0.36:11000", d.target());
        assertEquals("/?action=proxy-adb&udid=x", d.rest());
        assertTrue(d.tls());
        assertFalse(d.raw());
        ProxyRoute p = ProxyRoute.parse("/p/farm1/10.0.0.5");
        assertEquals("/", p.rest());
        assertFalse(p.tls());
        ProxyRoute t = ProxyRoute.parse("/t/farm%201/10.0.0.36:22");
        assertTrue(t.raw());
        assertEquals("farm 1", t.agentId());
        ProxyRoute pk = ProxyRoute.parse("/pk/farm1/127.0.0.1:8080/static/x.js");
        assertEquals("/pk/farm1/127.0.0.1:8080/static/x.js", pk.rest());
        assertNull(ProxyRoute.parse("/healthz"));
        assertNull(ProxyRoute.parse("/d/farm1/bad host/"));
    }

    @Test
    void allowlist() {
        assertEquals(0x0a000000L, TunnelAgent.ipToInt("10.0.0.0"));
        assertEquals(-1, TunnelAgent.ipToInt("10.0.0"));
        assertEquals(-1, TunnelAgent.ipToInt("10.0.0.256"));
    }

    @Test
    void requestHeadRewrite() {
        io.hydrangea.http.Headers h = new io.hydrangea.http.Headers().add("Host", "relay").add("Connection", "keep-alive")
                .add("Transfer-Encoding", "chunked").add("X-Keep", "1");
        io.hydrangea.http.HttpRequest req = new io.hydrangea.http.HttpRequest("POST", "/d/a/h:1/x", "HTTP/1.1", h, "body".getBytes(StandardCharsets.UTF_8));
        String head = new String(RelayServer.buildRequestHead(req, ProxyRoute.parse(req.target()), false), StandardCharsets.ISO_8859_1);
        assertEquals("POST /x HTTP/1.1\r\nHost: h:1\r\nX-Keep: 1\r\nContent-Length: 4\r\nConnection: close\r\n\r\n", head);
        io.hydrangea.http.Headers u = new io.hydrangea.http.Headers().add("Host", "relay").add("Connection", "Upgrade").add("Upgrade", "websocket");
        io.hydrangea.http.HttpRequest up = new io.hydrangea.http.HttpRequest("GET", "/d/a/h:1/?a=1", "HTTP/1.1", u, new byte[0]);
        String uh = new String(RelayServer.buildRequestHead(up, ProxyRoute.parse(up.target()), true), StandardCharsets.ISO_8859_1);
        assertEquals("GET /?a=1 HTTP/1.1\r\nHost: h:1\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n\r\n", uh);
    }
}
