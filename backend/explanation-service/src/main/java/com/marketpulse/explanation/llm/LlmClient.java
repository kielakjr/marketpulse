package com.marketpulse.explanation.llm;

import com.marketpulse.common.alert.AnomalyAlert;

public interface LlmClient {

    LlmResponse explain(AnomalyAlert alert);
}
