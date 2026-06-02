package com.marketpulse.ingestion_service.unit.binance;

import com.marketpulse.ingestion_service.binance.BinanceTradeMessage;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

import static org.assertj.core.api.Assertions.assertThat;

class BinanceTradeMessageTest {

    private final JsonMapper jsonMapper = JsonMapper.builder().build();

    @Nested
    class WhenDeserializingTradeMessage {

        private static final String TRADE_JSON = """
                {"e":"trade","E":1780135331774,"s":"BTCUSDT","t":6335284352,\
                "p":"73610.36000000","q":"0.00007000","T":1780135331773,"m":true,"M":true}""";

        private BinanceTradeMessage deserialized() {
            return jsonMapper.readValue(TRADE_JSON, BinanceTradeMessage.class);
        }

        @Test
        void mapsSymbol() {
            assertThat(deserialized().symbol()).isEqualTo("BTCUSDT");
        }

        @Test
        void mapsPrice() {
            assertThat(deserialized().price()).isEqualTo("73610.36000000");
        }

        @Test
        void mapsQuantity() {
            assertThat(deserialized().quantity()).isEqualTo("0.00007000");
        }

        @Test
        void mapsTradeId() {
            assertThat(deserialized().tradeId()).isEqualTo(6335284352L);
        }

        @Test
        void mapsTradeTime() {
            assertThat(deserialized().tradeTime()).isEqualTo(1780135331773L);
        }
    }

    @Nested
    class WhenMessageHasUnknownFields {

        @Test
        void ignoresThemInsteadOfFailing() {
            String json = """
                    {"s":"ETHUSDT","p":"2000.00","q":"1.5","t":1,"T":1780135331773,\
                    "unexpected":"value","another":123}""";

            BinanceTradeMessage message = jsonMapper.readValue(json, BinanceTradeMessage.class);

            assertThat(message.symbol()).isEqualTo("ETHUSDT");
        }
    }
}
