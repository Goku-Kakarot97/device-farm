package com.mservice.farmstream.relay;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A client URL on the relay: {@code /d/<agent>/<host[:port]>/<rest>} (TLS upstream), {@code /p/...} (plain TCP),
 * {@code /t/<agent>/<host[:port]>} (raw TCP over WebSocket, e.g. ssh), and the keep-path variants {@code /dk/}, {@code /pk/}
 * where the upstream receives the full original URL (Jenkins behind a prefix).
 */
public record ProxyRoute(String agentId, String target, String rest, boolean tls, boolean raw) {

    private static final Pattern URL = Pattern.compile("^/(d|p|t|dk|pk)/([^/]+)/([A-Za-z0-9.\\-]+(?::\\d{1,5})?)(/.*|$)");

    /** @param url the raw request target (path + query) */
    public static ProxyRoute parse(String url) {
        Matcher m = URL.matcher(url);
        if (!m.find()) {
            return null;
        }
        String mode = m.group(1);
        boolean keepPath = mode.equals("dk") || mode.equals("pk");
        String rest = m.group(4);
        return new ProxyRoute(
                URLDecoder.decode(m.group(2), StandardCharsets.UTF_8),
                m.group(3),
                keepPath ? url : (rest == null || rest.isEmpty() ? "/" : rest),
                mode.equals("d") || mode.equals("dk"),
                mode.equals("t"));
    }
}
