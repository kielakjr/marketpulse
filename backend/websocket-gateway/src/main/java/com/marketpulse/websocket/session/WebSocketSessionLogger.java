package com.marketpulse.websocket.session;

import lombok.extern.slf4j.Slf4j;
import org.springframework.context.event.EventListener;
import org.springframework.messaging.simp.SimpMessageHeaderAccessor;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.messaging.SessionConnectedEvent;
import org.springframework.web.socket.messaging.SessionDisconnectEvent;

@Slf4j
@Component
public class WebSocketSessionLogger {

    @EventListener
    public void onConnect(SessionConnectedEvent event) {
        String sessionId = SimpMessageHeaderAccessor.getSessionId(event.getMessage().getHeaders());
        log.info("WebSocket client connected: sessionId={}", sessionId);
    }

    @EventListener
    public void onDisconnect(SessionDisconnectEvent event) {
        log.info("WebSocket client disconnected: sessionId={}, status={}",
                event.getSessionId(), event.getCloseStatus());
    }
}
