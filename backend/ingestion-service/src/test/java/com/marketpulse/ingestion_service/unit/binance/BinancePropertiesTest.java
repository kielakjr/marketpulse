package com.marketpulse.ingestion_service.unit.binance;

import com.marketpulse.ingestion_service.binance.BinanceProperties;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.context.ConfigurationPropertiesAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import static org.assertj.core.api.Assertions.assertThat;

class BinancePropertiesTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withConfiguration(org.springframework.boot.autoconfigure.AutoConfigurations.of(
                    ConfigurationPropertiesAutoConfiguration.class))
            .withUserConfiguration(TestConfig.class);

    @EnableConfigurationProperties(BinanceProperties.class)
    static class TestConfig {
    }

    @Nested
    class WhenSymbolsAreConfigured {

        @Test
        void bindsThemInOrder() {
            contextRunner
                    .withPropertyValues("binance.symbols[0]=btcusdt", "binance.symbols[1]=ethusdt")
                    .run(context -> assertThat(context.getBean(BinanceProperties.class).symbols())
                            .containsExactly("btcusdt", "ethusdt"));
        }
    }

    @Nested
    class WhenNoSymbolsAreConfigured {

        @Test
        void bindsToNull() {
            contextRunner.run(context -> assertThat(context.getBean(BinanceProperties.class).symbols())
                    .isNull());
        }
    }
}
