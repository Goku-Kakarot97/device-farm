package com.mservice.farmstream;

import com.mservice.farmstream.device.DeviceServer;
import com.mservice.farmstream.relay.TunnelAgent;
import com.mservice.farmstream.util.Config;
import io.hydrangea.EventLoopGroup;
import io.hydrangea.log.Logger;

/**
 * Farm-host process: the device streaming server (ws-scrcpy fork, port 11000) and the relay tunnel agent in one JVM.
 * {@code NO_DEVICE_SERVER=true} runs only the tunnel agent; {@code NO_TUNNEL=true} (or no RELAY_URL) runs only the device server.
 */
final class AgentMain {

    private static final Logger LOG = Logger.get(AgentMain.class);

    private AgentMain() {
    }

    static void run() throws Exception {
        EventLoopGroup group = new EventLoopGroup();
        DeviceServer deviceServer = null;
        if (!Config.getBool("NO_DEVICE_SERVER", false)) {
            deviceServer = new DeviceServer(group);
            deviceServer.start();
        }
        TunnelAgent agent = null;
        if (!Config.getBool("NO_TUNNEL", false)) {
            agent = new TunnelAgent(group);
            if (agent.configured()) {
                agent.start();
            } else {
                LOG.warn("RELAY_URL not set: running without the relay tunnel");
                agent = null;
            }
        }
        if (deviceServer == null && agent == null) {
            LOG.error("nothing to run: both the device server and the tunnel are disabled");
            System.exit(1);
        }
        DeviceServer ds = deviceServer;
        TunnelAgent ta = agent;
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("shutting down");
            if (ta != null) {
                ta.stop();
            }
            if (ds != null) {
                ds.stop();
            }
            group.close();
        }));
        Thread.currentThread().join();
    }
}
