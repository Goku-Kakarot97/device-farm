package com.mservice.farmstream.device;

import com.mservice.farmstream.adb.AdbClient;
import io.hydrangea.log.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * Pushes and runs the bundled ws-scrcpy server (1.19-ws6) on a device and finds its pid.
 * Port of streaming-device/src/server/goog-device/ScrcpyServer.ts and common/Constants.ts.
 */
public final class ScrcpyServer {

    private static final Logger LOG = Logger.get(ScrcpyServer.class);

    public static final String SERVER_PACKAGE = "com.genymobile.scrcpy.Server";
    public static final int SERVER_PORT = 8886;
    public static final String SERVER_VERSION = "1.19-ws6";
    public static final String SERVER_TYPE = "web";
    public static final String LOG_LEVEL = "ERROR";
    public static final boolean LISTENS_ON_ALL_INTERFACES = true;
    public static final String SERVER_PROCESS_NAME = "app_process";

    private static final String TEMP_PATH = "/data/local/tmp/";
    private static final String FILE_NAME = "scrcpy-server.jar";
    private static final String PID_FILE_PATH = "/data/local/tmp/ws_scrcpy.pid";
    private static final String ARGS_STRING = "/ " + SERVER_PACKAGE + " " + SERVER_VERSION + " " + SERVER_TYPE + " " + LOG_LEVEL + " "
            + SERVER_PORT + " " + LISTENS_ON_ALL_INTERFACES + " 2>&1 > /dev/null";
    private static final String RUN_COMMAND = "CLASSPATH=" + TEMP_PATH + FILE_NAME + " nohup app_process " + ARGS_STRING;

    private static volatile byte[] serverJar;

    private ScrcpyServer() {
    }

    private static byte[] serverJar() throws IOException {
        byte[] jar = serverJar;
        if (jar == null) {
            try (InputStream in = ScrcpyServer.class.getResourceAsStream("/scrcpy-server.jar")) {
                if (in == null) {
                    throw new IOException("scrcpy-server.jar missing from the classpath");
                }
                jar = in.readAllBytes();
            }
            serverJar = jar;
        }
        return jar;
    }

    private static void copyServer(AndroidDevice device, AdbClient adb) throws IOException {
        adb.push(device.udid, serverJar(), TEMP_PATH + FILE_NAME);
    }

    /** Pids of running servers of our version; older compatible ("-ws") versions are killed on sight. */
    public static List<Integer> getServerPid(AndroidDevice device) throws IOException {
        if (!device.isConnected()) {
            return null;
        }
        List<Integer> list = device.getPidOf(SERVER_PROCESS_NAME);
        if (list == null || list.isEmpty()) {
            return null;
        }
        List<Integer> serverPids = new ArrayList<>();
        for (int pid : list) {
            String output;
            try {
                output = device.shell("cat /proc/" + pid + "/cmdline");
            } catch (IOException e) {
                continue;
            }
            List<String> args = new ArrayList<>(List.of(output.split("\0")));
            if (args.isEmpty() || !args.get(0).equals(SERVER_PROCESS_NAME)) {
                continue;
            }
            while (!args.isEmpty() && !args.get(0).equals(SERVER_PACKAGE)) {
                args.remove(0);
            }
            if (args.size() < 3) {
                continue;
            }
            String versionString = args.get(1);
            if (versionString.equals(SERVER_VERSION)) {
                serverPids.add(pid);
            } else {
                ServerVersion current = new ServerVersion(versionString);
                if (current.isCompatible() && new ServerVersion(SERVER_VERSION).gt(current)) {
                    LOG.info("{} found old server version running (PID: {}, Version: {}); killing it", device.tag, pid, versionString);
                    try {
                        device.killProcess(pid);
                    } catch (IOException ignored) {
                        // best effort
                    }
                }
            }
        }
        return serverPids;
    }

    /** Starts the server (pushing the jar first) and waits until its pid shows up. */
    public static List<Integer> run(AndroidDevice device, AdbClient adb) throws IOException {
        if (!device.isConnected()) {
            return null;
        }
        List<Integer> list = getServerPid(device);
        if (list != null && !list.isEmpty()) {
            return list;
        }
        copyServer(device, adb);
        boolean[] processExited = {false};
        AdbClient.ShellStream shell = adb.shellStream(device.udid, RUN_COMMAND);
        Thread.ofVirtual().name("scrcpy-" + device.udid).start(() -> {
            try (shell) {
                String out = new String(shell.input.readAllBytes()).trim();
                if (device.isConnected()) {
                    LOG.info("{} server exited: {}", device.tag, out);
                }
            } catch (IOException e) {
                LOG.info("{} server stream error: {}", device.tag, e.getMessage());
            } finally {
                processExited[0] = true;
            }
        });
        int tryCounter = 0;
        boolean lookPidFile = true;
        while (true) {
            if (processExited[0]) {
                return null;
            }
            long timeout = 500 + 100L * tryCounter;
            if (lookPidFile) {
                String content = device.shell("test -f " + PID_FILE_PATH + " && cat " + PID_FILE_PATH).trim();
                if (!content.isEmpty()) {
                    int pid;
                    try {
                        pid = Integer.parseInt(content.split("\\s+")[0]);
                    } catch (NumberFormatException e) {
                        pid = 0;
                    }
                    if (pid > 0) {
                        List<Integer> real = getServerPid(device);
                        if (real != null && real.contains(pid)) {
                            return real;
                        }
                        lookPidFile = false;
                    }
                }
            } else {
                List<Integer> real = getServerPid(device);
                if (real != null && !real.isEmpty()) {
                    return real;
                }
            }
            if (++tryCounter > 5) {
                throw new IOException("Failed to start server");
            }
            try {
                Thread.sleep(timeout);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("interrupted");
            }
        }
    }

    /** "1.19-ws6"-style version strings (port of ServerVersion.ts). */
    static final class ServerVersion {
        final String versionString;
        final String[] parts;
        final String suffix;
        final boolean compatible;

        ServerVersion(String versionString) {
            this.versionString = versionString;
            String[] temp = versionString.split("-", -1);
            String main = temp[0];
            StringBuilder sb = new StringBuilder();
            for (int i = 1; i < temp.length; i++) {
                if (i > 1) {
                    sb.append('-');
                }
                sb.append(temp[i]);
            }
            this.suffix = sb.toString();
            this.parts = main.isEmpty() ? new String[0] : main.split("\\.");
            this.compatible = suffix.startsWith("ws") && parts.length >= 2;
        }

        boolean gt(ServerVersion a) {
            if (versionString.equals(a.versionString)) {
                return false;
            }
            int min = Math.min(parts.length, a.parts.length);
            for (int i = 0; i < min; i++) {
                if (parts[i].compareTo(a.parts[i]) > 0) {
                    return true;
                }
            }
            if (parts.length != a.parts.length) {
                return parts.length > a.parts.length;
            }
            return suffix.compareTo(a.suffix) > 0;
        }

        boolean isCompatible() {
            return compatible;
        }
    }
}
