package com.marketpulse.explanation.persistence;

import org.springframework.data.mongodb.repository.MongoRepository;

import java.util.List;

public interface AnomalyRecordRepository extends MongoRepository<AnomalyRecord, String> {

    List<AnomalyRecord> findTop20BySymbolOrderByTimestampDesc(String symbol);

    List<AnomalyRecord> findTop50ByOrderByTimestampDesc();
}
