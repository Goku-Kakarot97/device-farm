package com.mservice.farmstream.device;

import com.mservice.farmstream.util.Json;
import io.hydrangea.log.Logger;
import io.hydrangea.websocket.WebSocket;
import io.hydrangea.websocket.WebSocketListener;

import java.io.IOException;
import java.util.Map;
import java.util.function.Consumer;

/**
 * {@code ?action=goog-device-list}: the ws-scrcpy device tracker protocol (a {@code devicelist} message, then a {@code device}
 * message per descriptor update; accepts ControlCenter commands). Port of goog-device/mw/DeviceTracker.ts.
 */
final class DeviceTrackerSocket implements WebSocketListener {

    private static final Logger LOG = Logger.get(DeviceTrackerSocket.class);

    private final DeviceRegistry registry;
    private WebSocket ws;
    private final Consumer<Map<String, Object>> onDescriptor = this::sendDevice;

    DeviceTrackerSocket(DeviceRegistry registry) {
        this.registry = registry;
    }

    @Override
    public void onOpen(WebSocket ws) {
        this.ws = ws;
        registry.onDescriptor(onDescriptor);
        ws.sendText(Json.stringify(Json.obj("id", -1, "type", "devicelist",
                "data", Json.obj("list", registry.getDevices(), "id", registry.getId(), "name", registry.getName()))));
    }

    private void sendDevice(Map<String, Object> device) {
        if (ws != null && ws.isOpen()) {
            ws.sendText(Json.stringify(Json.obj("id", -1, "type", "device",
                    "data", Json.obj("device", device, "id", registry.getId(), "name", registry.getName()))));
        }
    }

    @Override
    public void onText(WebSocket ws, String text) {
        Map<String, Object> command;
        try {
            command = Json.asObject(Json.parse(text));
        } catch (RuntimeException e) {
            LOG.error("[DeviceTracker] received message: {}. Error: {}", text, e.getMessage());
            return;
        }
        if (command == null) {
            return;
        }
        Thread.ofVirtual().start(() -> {
            try {
                registry.runCommand(command);
            } catch (IOException e) {
                LOG.error("[DeviceTracker] received message: {}. Error: {}", text, e.getMessage());
            }
        });
    }

    @Override
    public void onClose(WebSocket ws, int code, String reason) {
        registry.removeDescriptorListener(onDescriptor);
    }
}
