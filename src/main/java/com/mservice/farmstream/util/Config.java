package com.mservice.farmstream.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Configuration from environment variables, overridable with {@code --KEY=value} command line flags
 * (flags win, as in the Node version's cli.ts).
 */
public final class Config {

    private static final Pattern FLAG = Pattern.compile("^--([A-Za-z_][A-Za-z0-9_]*)=(.*)$");
    private static final Map<String, String> FLAGS = new HashMap<>();

    private Config() {
    }

    /** Stores every {@code --KEY=value} argument and returns the remaining (positional) arguments. */
    public static List<String> applyArgs(String[] args) {
        List<String> rest = new ArrayList<>();
        for (String arg : args) {
            Matcher m = FLAG.matcher(arg);
            if (m.matches()) {
                FLAGS.put(m.group(1), m.group(2));
            } else {
                rest.add(arg);
            }
        }
        return rest;
    }

    public static String get(String key, String def) {
        String v = FLAGS.get(key);
        if (v == null) {
            v = System.getenv(key);
        }
        return v == null || v.isEmpty() ? def : v;
    }

    public static String get(String key) {
        return get(key, null);
    }

    public static int getInt(String key, int def) {
        String v = get(key, null);
        if (v == null) {
            return def;
        }
        try {
            return Integer.parseInt(v.trim());
        } catch (NumberFormatException e) {
            return def;
        }
    }

    public static boolean getBool(String key, boolean def) {
        String v = get(key, null);
        if (v == null) {
            return def;
        }
        v = v.trim().toLowerCase(Locale.ROOT);
        return v.equals("1") || v.equals("true") || v.equals("yes");
    }
}
