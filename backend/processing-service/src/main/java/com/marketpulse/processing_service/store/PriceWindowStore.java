package com.marketpulse.processing_service.store;

import com.marketpulse.processing_service.indicator.PriceWindow;

import java.util.Optional;

/**
 * Durable storage for the rolling {@link PriceWindow} of each symbol.
 * Implementations must not let storage failures break the tick pipeline.
 */
public interface PriceWindowStore {

    /**
     * Loads the persisted window for a symbol, or an empty Optional if none is
     * stored or storage is unavailable.
     */
    Optional<PriceWindow> load(String symbol);

    /**
     * Persists the current window for a symbol. Failures are swallowed so that
     * processing can continue.
     */
    void save(String symbol, PriceWindow window);
}
