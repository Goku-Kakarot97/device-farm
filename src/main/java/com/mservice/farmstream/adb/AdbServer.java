package com.mservice.farmstream.adb;

import com.mservice.farmstream.util.Config;
import io.hydrangea.log.Logger;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Starts the local adb server when it is gone. {@link AdbClient} speaks to the server over TCP and never spawns a binary,
 * so without this a dead adb server leaves the device list empty until someone runs {@code adb start-server} by hand.
 * <p>
 * Env: {@code ADB_AUTOSTART} (true) to enable, {@code ADB_BIN} to point at the adb binary; otherwise it is looked up on
 * PATH, then in {@code $ANDROID_HOME}/{@code $ANDROID_SDK_ROOT}/platform-tools and the default SDK location of the user.
 * Only used when the server is on this machine (ADB_HOST loopback).
 */
public final class AdbServer {

    private static final Logger LOG = Logger.get(AdbServer.class);
    private static final long START_TIMEOUT_SECONDS = 20;

    private AdbServer() {
    }

    /** Whether auto start is enabled and the adb server lives on this machine. */
    public static boolean canStart(String adbHost) {
        return Config.getBool("ADB_AUTOSTART", true)
                && (adbHost.equals("127.0.0.1") || adbHost.equals("localhost") || adbHost.equals("::1"));
    }

    /** Runs {@code adb start-server} and reports success; failures are logged, never thrown. */
    public static boolean start(int adbPort) {
        Path adb = resolveBinary();
        if (adb == null) {
            LOG.error("adb server is down and no adb binary found: set ADB_BIN or add adb to PATH (or start it by hand: adb start-server)");
            return false;
        }
        LOG.warn("adb server is down; running {} start-server", adb);
        try {
            Process p = new ProcessBuilder(adb.toString(), "-P", Integer.toString(adbPort), "start-server")
                    .redirectErrorStream(true)
                    .start();
            String output = new String(p.getInputStream().readAllBytes()).trim();
            if (!p.waitFor(START_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                p.destroyForcibly();
                LOG.error("adb start-server did not finish in {}s", START_TIMEOUT_SECONDS);
                return false;
            }
            if (p.exitValue() != 0) {
                LOG.error("adb start-server exited with {}: {}", p.exitValue(), output);
                return false;
            }
            LOG.info("adb server started{}", output.isEmpty() ? "" : " (" + output + ")");
            return true;
        } catch (IOException e) {
            LOG.error("cannot run adb start-server: {}", e.getMessage());
            return false;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    /** The adb binary to use, or null when none exists. */
    static Path resolveBinary() {
        return resolveBinary(Config.get("ADB_BIN"), System.getenv("PATH"), System.getenv("ANDROID_HOME"),
                System.getenv("ANDROID_SDK_ROOT"), System.getProperty("user.home"));
    }

    static Path resolveBinary(String adbBin, String pathEnv, String androidHome, String androidSdkRoot, String userHome) {
        List<Path> candidates = new ArrayList<>();
        if (adbBin != null && !adbBin.isBlank()) {
            candidates.add(Path.of(adbBin));
        }
        if (pathEnv != null) {
            for (String dir : pathEnv.split(java.io.File.pathSeparator)) {
                if (!dir.isBlank()) {
                    candidates.add(Path.of(dir, "adb"));
                }
            }
        }
        for (String sdk : new String[] {androidHome, androidSdkRoot}) {
            if (sdk != null && !sdk.isBlank()) {
                candidates.add(Path.of(sdk, "platform-tools", "adb"));
            }
        }
        if (userHome != null) {
            candidates.add(Path.of(userHome, "Library", "Android", "sdk", "platform-tools", "adb")); // macOS default
            candidates.add(Path.of(userHome, "Android", "Sdk", "platform-tools", "adb")); // Linux default
        }
        for (Path c : candidates) {
            if (Files.isRegularFile(c) && Files.isExecutable(c)) {
                return c;
            }
        }
        return null;
    }
}
