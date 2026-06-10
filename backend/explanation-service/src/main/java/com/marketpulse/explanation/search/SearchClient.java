package com.marketpulse.explanation.search;

import com.marketpulse.common.alert.AnomalyAlert;

import java.util.List;

public interface SearchClient {

    /** Returns at most a handful of results, or an empty list on any failure. */
    List<SearchResult> search(AnomalyAlert alert);
}
