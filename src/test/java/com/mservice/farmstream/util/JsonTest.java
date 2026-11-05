package com.mservice.farmstream.util;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JsonTest {

    @Test
    void roundTrip() {
        String text = "{\"a\":1,\"b\":[true,null,\"x\\n\\u00e9\"],\"c\":{\"d\":-2.5},\"e\":\"\"}";
        Object parsed = Json.parse(text);
        Map<String, Object> m = Json.asObject(parsed);
        assertEquals(1L, m.get("a"));
        List<Object> b = Json.asList(m.get("b"));
        assertEquals(Boolean.TRUE, b.get(0));
        assertNull(b.get(1));
        assertEquals("x\né", b.get(2));
        assertEquals(-2.5, Json.num(Json.asObject(m.get("c")), "d"));
        assertEquals("{\"a\":1,\"b\":[true,null,\"x\\né\"],\"c\":{\"d\":-2.5},\"e\":\"\"}", Json.stringify(parsed));
    }

    @Test
    void helpers() {
        Map<String, Object> m = Json.obj("s", "v", "n", 3, "t", true);
        assertEquals("v", Json.str(m, "s"));
        assertEquals(3, Json.num(m, "n"));
        assertTrue(Json.bool(m, "t"));
        assertNull(Json.str(m, "n"));
        assertEquals("[{\"x\":1}]", Json.stringify(List.of(Json.obj("x", 1))));
    }

    @Test
    void rejectsGarbage() {
        assertThrows(IllegalArgumentException.class, () -> Json.parse("{bad"));
        assertThrows(IllegalArgumentException.class, () -> Json.parse("[1,]"));
        assertThrows(IllegalArgumentException.class, () -> Json.parse("{} x"));
    }
}
