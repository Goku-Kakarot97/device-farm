package com.mservice.farmstream.device;

import com.mservice.farmstream.adb.AdbClient;
import com.mservice.farmstream.util.Json;
import io.hydrangea.log.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * One Android device seen by the tracker: its descriptor (properties, network interfaces, scrcpy server pid), and the
 * scrcpy server life cycle on it. Port of streaming-device/src/server/goog-device/Device.ts. All ADB calls block and run
 * on virtual threads; the descriptor is published through {@code onUpdate} (throttled to 300 ms).
 */
public final class AndroidDevice {

    private static final Logger LOG = Logger.get(AndroidDevice.class);
    private static final long INITIAL_UPDATE_TIMEOUT = 1500;
    private static final int MAX_UPDATES_COUNT = 7;
    private static final long THROTTLE_MS = 300;
    private static final ScheduledExecutorService TIMERS = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "device-timers");
        t.setDaemon(true);
        return t;
    });
    static final List<String> PROPERTIES = List.of("ro.product.cpu.abi", "ro.product.manufacturer", "ro.product.model",
            "ro.build.version.release", "ro.build.version.sdk", "wifi.interface");
    private static final Pattern PROC_CMDLINE = Pattern.compile("/proc/([0-9]+)/cmdline");

    private enum PidDetection { UNKNOWN, PIDOF, GREP_PS, GREP_PS_A, LS_PROC }

    public record NetInterface(String name, String ipv4) implements Json.Jsonable {
        @Override
        public Object toJson() {
            return Json.obj("name", name, "ipv4", ipv4);
        }
    }

    public final String udid;
    public final String tag;
    private final AdbClient adb;
    private final Consumer<AndroidDevice> onUpdate;
    private volatile boolean connected = true;
    private volatile PidDetection pidDetection = PidDetection.UNKNOWN;
    private volatile Map<String, String> properties;
    private volatile boolean spawnServer = true;

    // descriptor
    private volatile String state;
    private volatile List<NetInterface> interfaces = List.of();
    private volatile int pid = -1;
    private final Map<String, String> props = new LinkedHashMap<>();
    private volatile long lastUpdateTimestamp;

    // info refresh bookkeeping
    private boolean updateScheduled;
    private boolean updating;
    private long updateTimeout = INITIAL_UPDATE_TIMEOUT;
    private int updateCount;
    private long lastEmit;
    private boolean throttleScheduled;

    public AndroidDevice(String udid, String state, AdbClient adb, Consumer<AndroidDevice> onUpdate) {
        this.udid = udid;
        this.tag = "[" + udid + "]";
        this.adb = adb;
        this.onUpdate = onUpdate;
        for (String p : PROPERTIES) {
            props.put(p, "");
        }
        setState(state);
    }

    public synchronized Map<String, Object> descriptor() {
        Map<String, Object> d = new LinkedHashMap<>();
        d.put("udid", udid);
        d.put("state", state);
        d.put("interfaces", new ArrayList<>(interfaces));
        d.put("pid", pid);
        d.put("wifi.interface", props.get("wifi.interface"));
        d.put("ro.build.version.release", props.get("ro.build.version.release"));
        d.put("ro.build.version.sdk", props.get("ro.build.version.sdk"));
        d.put("ro.product.manufacturer", props.get("ro.product.manufacturer"));
        d.put("ro.product.model", props.get("ro.product.model"));
        d.put("ro.product.cpu.abi", props.get("ro.product.cpu.abi"));
        d.put("last.update.timestamp", lastUpdateTimestamp);
        return d;
    }

    public void setState(String state) {
        if (state.equals("device")) {
            connected = true;
            properties = null;
        } else {
            connected = false;
        }
        this.state = state;
        emitUpdate(true);
        AdbClient.async(this::fetchDeviceInfo);
    }

    public boolean isConnected() {
        return connected;
    }

    public int pid() {
        return pid;
    }

    // ---- shell helpers ----

    public String shell(String command) throws IOException {
        return adb.shell(udid, command).trim();
    }

    public String killProcess(int pid) throws IOException {
        return shell("kill " + pid);
    }

    public Map<String, String> getProperties() throws IOException {
        Map<String, String> p = properties;
        if (p != null) {
            return p;
        }
        if (!connected) {
            return null;
        }
        p = adb.getProperties(udid);
        properties = p;
        return p;
    }

    public List<NetInterface> getNetInterfaces() throws IOException {
        if (!connected) {
            return List.of();
        }
        List<NetInterface> list = new ArrayList<>();
        String output = shell("ip -4 -f inet -o a | grep 'scope global'");
        for (String line : output.split("\n")) {
            if (line.isBlank()) {
                continue;
            }
            String[] temp = line.trim().split("\\s+");
            if (temp.length < 4) {
                continue;
            }
            list.add(new NetInterface(temp[1], temp[3].split("/")[0]));
        }
        list.sort((a, b) -> a.name().compareTo(b.name()));
        return list;
    }

    // ---- pid detection ----

    public List<Integer> getPidOf(String processName) throws IOException {
        if (!connected) {
            return null;
        }
        if (pidDetection == PidDetection.UNKNOWN) {
            pidDetection = findDetectionVariant();
        }
        return switch (pidDetection) {
            case PIDOF -> pidOf(processName);
            case GREP_PS -> grepPs(processName, "ps | grep " + processName);
            case GREP_PS_A -> grepPs(processName, "ps -A | grep " + processName);
            default -> listProc(processName);
        };
    }

    private List<Integer> pidOf(String processName) {
        List<Integer> list = new ArrayList<>();
        try {
            for (String s : shell("pidof " + processName).split("\\s+")) {
                try {
                    list.add(Integer.parseInt(s.trim()));
                } catch (NumberFormatException ignored) {
                    // not a pid
                }
            }
        } catch (IOException e) {
            return list;
        }
        return list;
    }

    private List<Integer> grepPs(String processName, String command) {
        List<Integer> list = new ArrayList<>();
        String output;
        try {
            output = shell(command);
        } catch (IOException e) {
            return list;
        }
        for (String line : output.split("\n")) {
            String[] cols = line.trim().split("\\s+");
            if (cols.length > 1 && cols[cols.length - 1].equals(processName)) {
                try {
                    list.add(Integer.parseInt(cols[1]));
                } catch (NumberFormatException ignored) {
                    // header line
                }
            }
        }
        return list;
    }

    private List<Integer> listProc(String processName) throws IOException {
        String find = "find /proc -maxdepth 2 -name cmdline  2>/dev/null";
        String lines = shell("for L in `" + find + "`; do grep -sae '^" + processName + "' $L 2>&1 >/dev/null && echo $L; done");
        List<Integer> list = new ArrayList<>();
        for (String line : lines.split("\n")) {
            Matcher m = PROC_CMDLINE.matcher(line.trim());
            if (m.find()) {
                list.add(Integer.parseInt(m.group(1)));
            }
        }
        return list;
    }

    private boolean executedWithoutError(String command) {
        try {
            return Integer.parseInt(shell(command).trim()) == 0;
        } catch (IOException | NumberFormatException e) {
            return false;
        }
    }

    private boolean hasPidOf() {
        if (!executedWithoutError("which pidof 2>&1 >/dev/null && echo $?")) {
            return false;
        }
        try {
            String[] lines = shell("echo $PPID; pidof init").split("\n");
            if (lines.length < 2) {
                return false;
            }
            String parentPid = lines[0].replace("\r", "").trim();
            List<String> list = List.of(lines[1].trim().split("\\s+"));
            return !list.contains(parentPid) && list.contains("1");
        } catch (IOException e) {
            return false;
        }
    }

    private PidDetection findDetectionVariant() {
        if (hasPidOf()) {
            return PidDetection.PIDOF;
        }
        if (executedWithoutError("ps -A | grep init 2>&1 >/dev/null; echo $?")) {
            return PidDetection.GREP_PS_A;
        }
        if (executedWithoutError("ps | grep init 2>&1 >/dev/null; echo $?")) {
            return PidDetection.GREP_PS;
        }
        return PidDetection.LS_PROC;
    }

    // ---- info refresh ----

    private void scheduleInfoUpdate() {
        synchronized (this) {
            if (updateScheduled) {
                return;
            }
            if (++updateCount > MAX_UPDATES_COUNT) {
                LOG.error("{} the maximum number of attempts to fetch device info has been reached", tag);
                return;
            }
            updateScheduled = true;
            TIMERS.schedule(() -> {
                synchronized (this) {
                    updateScheduled = false;
                }
                AdbClient.async(this::fetchDeviceInfo);
            }, updateTimeout, TimeUnit.MILLISECONDS);
            updateTimeout *= 2;
        }
    }

    private void fetchDeviceInfo() {
        if (!connected) {
            synchronized (this) {
                updateCount = 0;
                updateTimeout = INITIAL_UPDATE_TIMEOUT;
            }
            emitUpdate(true);
            return;
        }
        synchronized (this) {
            if (updating) {
                return;
            }
            updating = true;
        }
        int failed = 0;
        try {
            try {
                Map<String, String> p = getProperties();
                if (p == null) {
                    failed++;
                } else {
                    boolean changed = false;
                    synchronized (this) {
                        for (String name : PROPERTIES) {
                            String value = p.getOrDefault(name, "");
                            if (!value.equals(props.get(name))) {
                                changed = true;
                                props.put(name, value);
                            }
                        }
                    }
                    if (changed) {
                        emitUpdate(true);
                    }
                }
            } catch (IOException e) {
                failed++;
            }
            try {
                if (updateInterfaces().isEmpty()) {
                    failed++;
                }
            } catch (IOException e) {
                failed++;
            }
            try {
                if (spawnServer) {
                    startServer();
                } else {
                    getServerPid();
                }
                if (pid == -1 && spawnServer) {
                    failed++;
                }
            } catch (IOException e) {
                failed++;
            }
        } finally {
            synchronized (this) {
                updating = false;
            }
        }
        if (failed == 0) {
            synchronized (this) {
                updateCount = 0;
                updateTimeout = INITIAL_UPDATE_TIMEOUT;
            }
        } else {
            scheduleInfoUpdate();
        }
    }

    private void emitUpdate(boolean setUpdateTime) {
        long now = System.currentTimeMillis();
        boolean fire = false;
        long delay = 0;
        synchronized (this) {
            if (setUpdateTime) {
                lastUpdateTimestamp = now;
            }
            long since = now - lastEmit;
            if (since > THROTTLE_MS) {
                lastEmit = now;
                fire = true;
            } else if (!throttleScheduled) {
                throttleScheduled = true;
                delay = THROTTLE_MS - since;
            } else {
                return;
            }
        }
        if (fire) {
            onUpdate.accept(this);
            return;
        }
        TIMERS.schedule(() -> {
            synchronized (this) {
                throttleScheduled = false;
            }
            emitUpdate(false);
        }, delay, TimeUnit.MILLISECONDS);
    }

    // ---- scrcpy server ----

    /** @return the running scrcpy server's pid, or -1 */
    public int getServerPid() throws IOException {
        List<Integer> pids = ScrcpyServer.getServerPid(this);
        int p = pids == null || pids.isEmpty() ? -1 : pids.get(0);
        if (pid != p) {
            pid = p;
            emitUpdate(true);
        }
        return p;
    }

    public List<NetInterface> updateInterfaces() throws IOException {
        List<NetInterface> fresh = getNetInterfaces();
        if (!fresh.equals(interfaces)) {
            interfaces = fresh;
            emitUpdate(true);
        }
        return interfaces;
    }

    public void killServer(int requestedPid) throws IOException {
        spawnServer = false;
        int realPid = getServerPid();
        if (realPid < 0) {
            return;
        }
        if (realPid != requestedPid) {
            LOG.error("{} requested to kill server with PID {}. Real server PID is {}.", tag, requestedPid, realPid);
        }
        String output = killProcess(realPid);
        if (!output.isEmpty()) {
            LOG.info("{} kill server: \"{}\"", tag, output);
        }
        pid = -1;
        emitUpdate(true);
    }

    /** @return the server pid after (re)starting it, or -1 */
    public int startServer() throws IOException {
        spawnServer = true;
        int p = getServerPid();
        if (p >= 0) {
            return p;
        }
        List<Integer> started = ScrcpyServer.run(this, adb);
        if (started != null && !started.isEmpty()) {
            LOG.info("{} start server: pid {}", tag, started);
        }
        return getServerPid();
    }
}
