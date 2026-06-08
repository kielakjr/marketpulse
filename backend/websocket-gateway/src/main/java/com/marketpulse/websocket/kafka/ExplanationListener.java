package com.marketpulse.websocket.kafka;

import com.marketpulse.common.explanation.AnomalyExplanation;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class ExplanationListener {

    private final SimpMessagingTemplate messagingTemplate;

    @KafkaListener(topics = "market.explanations", groupId = "websocket-gateway")
    public void onExplanation(AnomalyExplanation explanation) {
        messagingTemplate.convertAndSend(
                "/topic/explanations/" + explanation.symbol(), explanation);
    }
}
