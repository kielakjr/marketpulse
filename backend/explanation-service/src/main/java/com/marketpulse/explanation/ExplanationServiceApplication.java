package com.marketpulse.explanation;

import com.marketpulse.explanation.search.SearxngProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@SpringBootApplication
@EnableConfigurationProperties(SearxngProperties.class)
public class ExplanationServiceApplication {

    static void main(String[] args) {
        SpringApplication.run(ExplanationServiceApplication.class, args);
    }
}
