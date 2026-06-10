package com.marketpulse.explanation.llm;

public interface LlmClient {

    /** Sends a finished prompt and returns the raw model text (expected to be JSON). */
    String complete(String prompt);
}
