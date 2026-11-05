package com.mservice.farmstream.relay;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Wire protocol for multiplexing many logical byte streams over one WebSocket tunnel between relay-server and
 * relay-agent (identical to stream-relay/src/protocol.ts, so Node and Java ends interoperate).
 * <pre>
 *   [1 byte type][4 bytes streamId, big-endian, assigned by the relay-server][payload...]
 * </pre>
 */
public record Frame(int type, int streamId, ByteBuffer payload) {

    /** server -> agent: open a TCP/TLS connection; payload = JSON {"target":"host:port","tls":true} */
    public static final int OPEN = 1;
    /** agent -> server: connection established */
    public static final int OPEN_OK = 2;
    /** agent -> server: connection failed; payload = utf8 error message */
    public static final int OPEN_FAIL = 3;
    /** both directions: raw bytes for the stream */
    public static final int DATA = 4;
    /** both directions: stream ended (payload = optional utf8 reason) */
    public static final int CLOSE = 5;

    public static final int HEADER_SIZE = 5;

    public static ByteBuffer encode(int type, int streamId, ByteBuffer payload) {
        int n = payload == null ? 0 : payload.remaining();
        ByteBuffer out = ByteBuffer.allocate(HEADER_SIZE + n);
        out.put((byte) type).putInt(streamId);
        if (n > 0) {
            out.put(payload.duplicate());
        }
        return out.flip();
    }

    public static ByteBuffer encode(int type, int streamId, byte[] payload) {
        return encode(type, streamId, payload == null ? null : ByteBuffer.wrap(payload));
    }

    public static ByteBuffer encode(int type, int streamId, String payload) {
        return encode(type, streamId, payload == null ? null : payload.getBytes(StandardCharsets.UTF_8));
    }

    public static ByteBuffer encode(int type, int streamId) {
        return encode(type, streamId, (ByteBuffer) null);
    }

    /** @return null when the message is too short to be a frame */
    public static Frame decode(ByteBuffer data) {
        if (data.remaining() < HEADER_SIZE) {
            return null;
        }
        int type = data.get() & 0xff;
        int streamId = data.getInt();
        return new Frame(type, streamId, data.slice());
    }

    public String payloadText() {
        return StandardCharsets.UTF_8.decode(payload.duplicate()).toString();
    }
}
