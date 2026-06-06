package com.marketpulse.websocket.kafka;

import com.marketpulse.common.alert.AnomalyAlert;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class AlertListener {

    private final SimpMessagingTemplate messagingTemplate;

    @KafkaListener(topics = "market.alerts", groupId = "websocket-gateway")
    public void onAlert(AnomalyAlert alert) {
        messagingTemplate.convertAndSend("/topic/alerts/" + alert.symbol(), alert);
    }
}
