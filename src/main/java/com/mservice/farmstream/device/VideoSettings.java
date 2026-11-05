package com.mservice.farmstream.device;

import com.mservice.farmstream.util.Json;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * scrcpy-server 1.19-ws6 video settings and their 35-byte big-endian binary form (CHANGE_STREAM_PARAMETERS payload).
 * Port of streaming-device/src/common/VideoSettings.ts.
 */
public record VideoSettings(
        Rect crop,
        int bitrate,
        Size bounds,
        int maxFps,
        int iFrameInterval,
        boolean sendFrameMeta,
        int lockedVideoOrientation,
        int displayId,
        String codecOptions,
        String encoderName) implements Json.Jsonable {

    public static final int BASE_BUFFER_LENGTH = 35;

    public record Rect(int left, int top, int right, int bottom) implements Json.Jsonable {
        @Override
        public Object toJson() {
            return Json.obj("left", left, "top", top, "right", right, "bottom", bottom);
        }
    }

    public record Size(int width, int height) implements Json.Jsonable {
        @Override
        public Object toJson() {
            return Json.obj("width", width, "height", height);
        }
    }

    public static VideoSettings fromBuffer(ByteBuffer b) {
        int bitrate = b.getInt();
        int maxFps = b.getInt();
        int iFrameInterval = b.get();
        int width = b.getShort();
        int height = b.getShort();
        int left = b.getShort();
        int top = b.getShort();
        int right = b.getShort();
        int bottom = b.getShort();
        boolean sendFrameMeta = b.get() != 0;
        int lockedVideoOrientation = b.get();
        int displayId = b.getInt();
        Size bounds = width != 0 && height != 0 ? new Size(width, height) : null;
        Rect crop = (left | top | right | bottom) != 0 ? new Rect(left, top, right, bottom) : null;
        String codecOptions = null;
        String encoderName = null;
        int codecLen = b.getInt();
        if (codecLen > 0) {
            byte[] bytes = new byte[codecLen];
            b.get(bytes);
            codecOptions = new String(bytes, StandardCharsets.UTF_8);
        }
        int encLen = b.getInt();
        if (encLen > 0) {
            byte[] bytes = new byte[encLen];
            b.get(bytes);
            encoderName = new String(bytes, StandardCharsets.UTF_8);
        }
        return new VideoSettings(crop, bitrate, bounds, maxFps, iFrameInterval, sendFrameMeta, lockedVideoOrientation, displayId, codecOptions, encoderName);
    }

    public ByteBuffer toBuffer() {
        byte[] codec = codecOptions == null || codecOptions.isEmpty() ? null : codecOptions.getBytes(StandardCharsets.UTF_8);
        byte[] enc = encoderName == null || encoderName.isEmpty() ? null : encoderName.getBytes(StandardCharsets.UTF_8);
        ByteBuffer b = ByteBuffer.allocate(BASE_BUFFER_LENGTH + (codec == null ? 0 : codec.length) + (enc == null ? 0 : enc.length));
        b.putInt(bitrate).putInt(maxFps).put((byte) iFrameInterval);
        b.putShort((short) (bounds == null ? 0 : bounds.width())).putShort((short) (bounds == null ? 0 : bounds.height()));
        b.putShort((short) (crop == null ? 0 : crop.left())).putShort((short) (crop == null ? 0 : crop.top()));
        b.putShort((short) (crop == null ? 0 : crop.right())).putShort((short) (crop == null ? 0 : crop.bottom()));
        b.put((byte) (sendFrameMeta ? 1 : 0)).put((byte) lockedVideoOrientation).putInt(displayId);
        b.putInt(codec == null ? 0 : codec.length);
        if (codec != null) {
            b.put(codec);
        }
        b.putInt(enc == null ? 0 : enc.length);
        if (enc != null) {
            b.put(enc);
        }
        return b.flip();
    }

    @Override
    public Object toJson() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("bitrate", bitrate);
        m.put("maxFps", maxFps);
        m.put("iFrameInterval", iFrameInterval);
        m.put("bounds", bounds);
        m.put("crop", crop);
        m.put("sendFrameMeta", sendFrameMeta);
        m.put("lockedVideoOrientation", lockedVideoOrientation);
        m.put("displayId", displayId);
        m.put("codecOptions", codecOptions);
        m.put("encoderName", encoderName);
        return m;
    }
}
