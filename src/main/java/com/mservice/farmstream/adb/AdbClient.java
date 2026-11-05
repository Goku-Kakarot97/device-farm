package com.mservice.farmstream.adb;

import com.mservice.farmstream.util.Config;
import io.hydrangea.log.Logger;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A small client for the ADB server protocol (what adbkit did for the Node version): device tracking, shell commands,
 * file push (sync), port forwarding, connect/disconnect, tcpip/usb. Calls block; run them on virtual threads.
 * The server is {@code ADB_HOST:ADB_PORT} (127.0.0.1:5037).
 */
public final class AdbClient {

    private static final Logger LOG = Logger.get(AdbClient.class);
    private static final int COMMAND_TIMEOUT_MS = 30_000;
    private static final Pattern PROP = Pattern.compile("^\\[([\\s\\S]*?)\\]: \\[([\\s\\S]*?)\\]\\r?$", Pattern.MULTILINE);

    public record DeviceEntry(String serial, String state) {
    }

    public record Forward(String serial, String local, String remote) {
    }

    /** A device-list tracker: {@link #onChange} receives every list the server pushes. */
    public interface TrackerListener {
        void onChange(List<DeviceEntry> devices);

        void onEnd(Throwable cause);
    }

    /** A long-running shell command whose output is read as it comes; {@link #close()} ends it. */
    public static final class ShellStream implements AutoCloseable {
        private final Socket socket;
        public final InputStream input;

        ShellStream(Socket socket, InputStream input) {
            this.socket = socket;
            this.input = input;
        }

        @Override
        public void close() {
            try {
                socket.close();
            } catch (IOException ignored) {
                // nothing to do
            }
        }
    }

    private final String host;
    private final int port;

    public AdbClient() {
        this(Config.get("ADB_HOST", "127.0.0.1"), Config.getInt("ADB_PORT", 5037));
    }

    public AdbClient(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public String host() {
        return host;
    }

    public int port() {
        return port;
    }

    // ---- connection plumbing ----

    private Socket open(int timeoutMs) throws IOException {
        Socket s = new Socket();
        s.setTcpNoDelay(true);
        s.connect(new InetSocketAddress(host, port), 5000);
        s.setSoTimeout(timeoutMs);
        return s;
    }

    private static void send(OutputStream out, String command) throws IOException {
        byte[] payload = command.getBytes(StandardCharsets.UTF_8);
        out.write(String.format("%04x", payload.length).getBytes(StandardCharsets.US_ASCII));
        out.write(payload);
        out.flush();
    }

    private static void readOkay(DataInputStream in, String what) throws IOException {
        byte[] status = new byte[4];
        in.readFully(status);
        String s = new String(status, StandardCharsets.US_ASCII);
        if (s.equals("OKAY")) {
            return;
        }
        if (s.equals("FAIL")) {
            String msg = readHexPrefixed(in);
            throw new IOException(what + ": " + (msg.isEmpty() ? "adb server answered FAIL (device not found or offline?)" : msg));
        }
        throw new IOException(what + ": unexpected reply \"" + s + "\"");
    }

    private static String readHexPrefixed(DataInputStream in) throws IOException {
        byte[] len = new byte[4];
        in.readFully(len);
        int n = Integer.parseInt(new String(len, StandardCharsets.US_ASCII), 16);
        byte[] data = new byte[n];
        in.readFully(data);
        return new String(data, StandardCharsets.UTF_8);
    }

    private static String readAll(InputStream in) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        in.transferTo(out);
        return out.toString(StandardCharsets.UTF_8);
    }

    /** Runs one {@code host:...} service that answers with a length-prefixed payload. */
    private String hostService(String service) throws IOException {
        try (Socket s = open(COMMAND_TIMEOUT_MS)) {
            DataInputStream in = new DataInputStream(s.getInputStream());
            send(s.getOutputStream(), service);
            readOkay(in, service);
            return readHexPrefixed(in);
        }
    }

    private Socket transport(String serial, int timeoutMs) throws IOException {
        Socket s = open(timeoutMs);
        try {
            DataInputStream in = new DataInputStream(s.getInputStream());
            send(s.getOutputStream(), "host:transport:" + serial);
            readOkay(in, "transport " + serial);
            return s;
        } catch (IOException e) {
            s.close();
            throw e;
        }
    }

    // ---- devices ----

    public List<DeviceEntry> listDevices() throws IOException {
        return parseDevices(hostService("host:devices"));
    }

    private static List<DeviceEntry> parseDevices(String text) {
        List<DeviceEntry> list = new ArrayList<>();
        for (String line : text.split("\n")) {
            line = line.trim();
            if (line.isEmpty()) {
                continue;
            }
            String[] cols = line.split("\\s+");
            if (cols.length >= 2) {
                list.add(new DeviceEntry(cols[0], cols[1]));
            }
        }
        return list;
    }

    /**
     * Keeps a {@code host:track-devices} connection open and reports every change until the server closes it (or
     * {@code Thread.interrupt()}). Blocks: call on its own virtual thread.
     */
    public void trackDevices(TrackerListener listener) {
        Socket s = null;
        try {
            s = open(0);
            DataInputStream in = new DataInputStream(s.getInputStream());
            send(s.getOutputStream(), "host:track-devices");
            readOkay(in, "track-devices");
            while (!Thread.currentThread().isInterrupted()) {
                listener.onChange(parseDevices(readHexPrefixed(in)));
            }
            listener.onEnd(null);
        } catch (IOException e) {
            listener.onEnd(e);
        } finally {
            if (s != null) {
                try {
                    s.close();
                } catch (IOException ignored) {
                    // closing
                }
            }
        }
    }

    // ---- shell ----

    /** Runs a shell command and returns everything it printed (stdout and stderr merged by adb). */
    public String shell(String serial, String command) throws IOException {
        return shell(serial, command, COMMAND_TIMEOUT_MS);
    }

    public String shell(String serial, String command, int timeoutMs) throws IOException {
        try (Socket s = transport(serial, timeoutMs)) {
            DataInputStream in = new DataInputStream(s.getInputStream());
            send(s.getOutputStream(), "shell:" + command);
            readOkay(in, "shell");
            return readAll(in);
        }
    }

    /** Starts a shell command and leaves it running; the caller reads {@link ShellStream#input} until EOF. */
    public ShellStream shellStream(String serial, String command) throws IOException {
        Socket s = transport(serial, 0);
        try {
            DataInputStream in = new DataInputStream(s.getInputStream());
            send(s.getOutputStream(), "shell:" + command);
            readOkay(in, "shell");
            return new ShellStream(s, in);
        } catch (IOException e) {
            s.close();
            throw e;
        }
    }

    public Map<String, String> getProperties(String serial) throws IOException {
        String out = shell(serial, "getprop");
        Map<String, String> props = new LinkedHashMap<>();
        Matcher m = PROP.matcher(out);
        while (m.find()) {
            props.put(m.group(1), m.group(2));
        }
        return props;
    }

    // ---- sync (push) ----

    /** Pushes a local file to the device (like {@code adb push}), mode 0644. */
    public void push(String serial, Path local, String remote) throws IOException {
        push(serial, Files.readAllBytes(local), remote);
    }

    public void push(String serial, byte[] content, String remote) throws IOException {
        try (Socket s = transport(serial, 60_000)) {
            DataInputStream in = new DataInputStream(s.getInputStream());
            OutputStream out = s.getOutputStream();
            send(out, "sync:");
            readOkay(in, "sync");
            syncRequest(out, "SEND", (remote + "," + 0644).getBytes(StandardCharsets.UTF_8));
            int off = 0;
            while (off < content.length) {
                int n = Math.min(64 * 1024, content.length - off);
                out.write("DATA".getBytes(StandardCharsets.US_ASCII));
                out.write(le32(n));
                out.write(content, off, n);
                off += n;
            }
            out.write("DONE".getBytes(StandardCharsets.US_ASCII));
            out.write(le32((int) (System.currentTimeMillis() / 1000)));
            out.flush();
            byte[] id = new byte[4];
            in.readFully(id);
            byte[] lenBytes = new byte[4];
            in.readFully(lenBytes);
            int len = ByteBuffer.wrap(lenBytes).order(ByteOrder.LITTLE_ENDIAN).getInt();
            String status = new String(id, StandardCharsets.US_ASCII);
            if (!status.equals("OKAY")) {
                byte[] msg = new byte[Math.max(0, len)];
                in.readFully(msg);
                throw new IOException("push " + remote + " failed: " + new String(msg, StandardCharsets.UTF_8));
            }
            out.write("QUIT".getBytes(StandardCharsets.US_ASCII));
            out.write(le32(0));
            out.flush();
        }
    }

    private static void syncRequest(OutputStream out, String id, byte[] payload) throws IOException {
        out.write(id.getBytes(StandardCharsets.US_ASCII));
        out.write(le32(payload.length));
        out.write(payload);
        out.flush();
    }

    private static byte[] le32(int v) {
        return ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(v).array();
    }

    // ---- forwards ----

    public List<Forward> listForwards() throws IOException {
        List<Forward> list = new ArrayList<>();
        for (String line : hostService("host:list-forward").split("\n")) {
            String[] cols = line.trim().split("\\s+");
            if (cols.length >= 3) {
                list.add(new Forward(cols[0], cols[1], cols[2]));
            }
        }
        return list;
    }

    /** Returns the local TCP port forwarded to {@code remote} on the device, creating the forward if needed. */
    public int forward(String serial, String remote) throws IOException {
        for (Forward f : listForwards()) {
            if (f.serial().equals(serial) && f.remote().equals(remote) && f.local().startsWith("tcp:")) {
                return Integer.parseInt(f.local().substring(4));
            }
        }
        int port = freePort();
        try (Socket s = open(COMMAND_TIMEOUT_MS)) {
            DataInputStream in = new DataInputStream(s.getInputStream());
            send(s.getOutputStream(), "host-serial:" + serial + ":forward:tcp:" + port + ";" + remote);
            readOkay(in, "forward");
            readOkay(in, "forward"); // the server answers twice: request accepted, forward established
        }
        return port;
    }

    private static int freePort() throws IOException {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }

    // ---- connection management ----

    /** {@code adb connect host:port}; returns the server's message ("connected to ...", "already connected ...", "failed ..."). */
    public String connect(String hostPort) throws IOException {
        return hostService("host:connect:" + hostPort);
    }

    public String disconnect(String hostPort) throws IOException {
        return hostService("host:disconnect:" + hostPort);
    }

    /** {@code adb tcpip <port>}: restarts adbd on the device listening on TCP. */
    public String tcpip(String serial, int port) throws IOException {
        return transportCommand(serial, "tcpip:" + port);
    }

    /** {@code adb usb}: restarts adbd on the device in USB mode. */
    public String usb(String serial) throws IOException {
        return transportCommand(serial, "usb:");
    }

    private String transportCommand(String serial, String command) throws IOException {
        try (Socket s = transport(serial, COMMAND_TIMEOUT_MS)) {
            DataInputStream in = new DataInputStream(s.getInputStream());
            send(s.getOutputStream(), command);
            readOkay(in, command);
            try {
                return readAll(in).trim();
            } catch (EOFException e) {
                return "";
            }
        }
    }

    /** Convenience for fire-and-forget shells where failures only deserve a log line. */
    public String shellSafe(String serial, String command) {
        try {
            return shell(serial, command);
        } catch (IOException e) {
            LOG.debug("shell \"{}\" on {} failed: {}", command, serial, e.getMessage());
            return "";
        }
    }

    /** Runs {@code body} on a virtual thread (the ADB client blocks). */
    public static void async(Runnable body) {
        Thread.ofVirtual().start(body);
    }

    public static void async(String name, Consumer<Void> body) {
        Thread.ofVirtual().name(name).start(() -> body.accept(null));
    }
}
