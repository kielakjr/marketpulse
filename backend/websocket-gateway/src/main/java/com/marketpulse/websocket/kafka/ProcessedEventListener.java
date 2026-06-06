package com.marketpulse.websocket.kafka;

import com.marketpulse.common.event.ProcessedEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class ProcessedEventListener {

    private final SimpMessagingTemplate messagingTemplate;

    @KafkaListener(topics = "market.processed", groupId = "websocket-gateway")
    public void onProcessedEvent(ProcessedEvent event) {
        messagingTemplate.convertAndSend("/topic/prices/" + event.symbol(), event);
    }
}
