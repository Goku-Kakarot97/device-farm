package com.mservice.farmstream.device;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class VideoSettingsTest {

    @Test
    void binaryRoundTripIsBigEndian() {
        VideoSettings v = new VideoSettings(null, 2_000_000, new VideoSettings.Size(800, 800), 30, 5, false, -1, 0, null, null);
        ByteBuffer b = v.toBuffer();
        assertEquals(VideoSettings.BASE_BUFFER_LENGTH, b.remaining());
        byte[] bytes = new byte[8];
        b.duplicate().get(bytes);
        assertEquals(0x00, bytes[0]);
        assertEquals(0x1e, bytes[1]);
        assertEquals((byte) 0x84, bytes[2]);
        assertEquals((byte) 0x80, bytes[3]);
        assertEquals(30, bytes[7]);
        VideoSettings back = VideoSettings.fromBuffer(b);
        assertEquals(v, back);
        assertNull(back.crop());
    }

    @Test
    void stringsAndCrop() {
        VideoSettings v = new VideoSettings(new VideoSettings.Rect(1, 2, 3, 4), 1, null, 2, 3, true, 1, 2, "opt", "enc");
        assertEquals(v, VideoSettings.fromBuffer(v.toBuffer()));
    }
}
