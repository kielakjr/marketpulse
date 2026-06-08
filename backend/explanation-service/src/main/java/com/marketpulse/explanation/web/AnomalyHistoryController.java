package com.marketpulse.explanation.web;

import com.marketpulse.explanation.persistence.AnomalyRecord;
import com.marketpulse.explanation.persistence.AnomalyRecordRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api/anomalies")
@RequiredArgsConstructor
public class AnomalyHistoryController {

    private final AnomalyRecordRepository repository;

    @GetMapping
    public List<AnomalyRecord> recent() {
        return repository.findTop50ByOrderByTimestampDesc();
    }

    @GetMapping("/{symbol}")
    public List<AnomalyRecord> forSymbol(@PathVariable String symbol) {
        return repository.findTop20BySymbolOrderByTimestampDesc(symbol);
    }
}
