package com.mservice.farmstream.device;

import com.mservice.farmstream.util.Json;
import io.hydrangea.websocket.WebSocket;
import io.hydrangea.websocket.WebSocketListener;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/** {@code ?action=record-status}: pushes {@code {type:"snapshot", statuses:[{device,status}]}} on every change. */
final class RecordingStatusSocket implements WebSocketListener {

    private final RecordingStatusService service;
    private WebSocket ws;
    private final Consumer<List<RecordingStatusService.PublicStatus>> onSnapshot = this::send;

    RecordingStatusSocket(RecordingStatusService service) {
        this.service = service;
    }

    @Override
    public void onOpen(WebSocket ws) {
        this.ws = ws;
        send(service.getPublicSnapshot());
        service.onSnapshot(onSnapshot);
    }

    private void send(List<RecordingStatusService.PublicStatus> statuses) {
        if (ws == null || !ws.isOpen()) {
            return;
        }
        List<Object> list = new ArrayList<>();
        for (RecordingStatusService.PublicStatus s : statuses) {
            list.add(Json.obj("device", s.device(), "status", s.status()));
        }
        ws.sendText(Json.stringify(Json.obj("type", "snapshot", "statuses", list)));
    }

    @Override
    public void onClose(WebSocket ws, int code, String reason) {
        service.removeListener(onSnapshot);
    }
}
