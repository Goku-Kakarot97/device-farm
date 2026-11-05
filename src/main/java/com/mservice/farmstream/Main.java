package com.mservice.farmstream;

import com.mservice.farmstream.relay.RelayServer;
import com.mservice.farmstream.relay.WsPipe;
import com.mservice.farmstream.util.Config;
import io.hydrangea.log.Level;
import io.hydrangea.log.Log;

import java.util.List;
import java.util.Locale;

/**
 * Entry point. Modes:
 * <pre>
 *   java -jar farm-stream.jar server            relay-server (k8s): accepts agent tunnels and client requests
 *   java -jar farm-stream.jar agent             farm host: device streaming server (port 11000) + tunnel agent dialing out to the relay
 *   java -jar farm-stream.jar wspipe [--insecure] wss://relay/t/agent/host:port    stdio <-> WebSocket bridge (ssh ProxyCommand)
 * </pre>
 * Every setting is an environment variable or a {@code --KEY=value} flag (the flag wins); see README.md.
 */
public final class Main {

    private Main() {
    }

    public static void main(String[] args) throws Exception {
        List<String> rest = Config.applyArgs(args);
        String mode = rest.isEmpty() ? "" : rest.get(0).toLowerCase(Locale.ROOT);
        configureLogging(mode.equals("wspipe"));
        switch (mode) {
            case "server" -> RelayServer.run();
            case "agent" -> AgentMain.run();
            case "wspipe" -> WsPipe.run(rest.subList(1, rest.size()));
            default -> {
                System.err.println("usage: farm-stream (server|agent|wspipe) [--KEY=value ...]");
                System.exit(2);
            }
        }
    }

    private static void configureLogging(boolean stderr) {
        Level level;
        try {
            level = Level.valueOf(Config.get("LOG_LEVEL", "INFO").toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            level = Level.INFO;
        }
        var cfg = Log.configure().level(level).level("io.hydrangea", level == Level.DEBUG ? Level.DEBUG : Level.WARN);
        if (stderr) {
            cfg.stderr(); // wspipe: stdout is the data channel
        } else {
            cfg.console();
        }
        cfg.apply();
    }
}
