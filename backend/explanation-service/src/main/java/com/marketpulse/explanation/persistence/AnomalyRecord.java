package com.marketpulse.explanation.persistence;

import com.marketpulse.common.explanation.AnomalySource;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

@Document(collection = "anomaly_records")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class AnomalyRecord {

    @Id
    private String id;
    private String symbol;
    private BigDecimal price;
    private double zScore;
    private String severity;
    private String explanation;
    private List<AnomalySource> sources;
    private Instant timestamp;
}
