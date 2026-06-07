package com.marketpulse.websocket.unit.session;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.marketpulse.websocket.session.WebSocketSessionLogger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.messaging.SessionConnectedEvent;
import org.springframework.web.socket.messaging.SessionDisconnectEvent;
import org.springframework.messaging.simp.stomp.StompCommand;
import org.springframework.messaging.simp.stomp.StompHeaderAccessor;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class WebSocketSessionLoggerTest {

    private final WebSocketSessionLogger sessionLogger = new WebSocketSessionLogger();

    private Logger logger;
    private ListAppender<ILoggingEvent> appender;

    @BeforeEach
    void setUp() {
        logger = (Logger) LoggerFactory.getLogger(WebSocketSessionLogger.class);
        appender = new ListAppender<>();
        appender.start();
        logger.addAppender(appender);
    }

    @AfterEach
    void tearDown() {
        logger.detachAppender(appender);
    }

    private Message<byte[]> messageWithSession(StompCommand command, String sessionId) {
        StompHeaderAccessor accessor = StompHeaderAccessor.create(command);
        accessor.setSessionId(sessionId);
        return MessageBuilder.createMessage(new byte[0], accessor.getMessageHeaders());
    }

    private List<ILoggingEvent> loggedMessages() {
        return appender.list;
    }

    @Test
    void logsConnectionWithSessionId() {
        var event = new SessionConnectedEvent(
                this, messageWithSession(StompCommand.CONNECTED, "sess-connect-1"));

        sessionLogger.onConnect(event);

        assertThat(loggedMessages())
                .singleElement()
                .satisfies(log -> assertThat(log.getFormattedMessage())
                        .contains("connected")
                        .contains("sess-connect-1"));
    }

    @Test
    void logsDisconnectionWithSessionId() {
        var event = new SessionDisconnectEvent(
                this,
                messageWithSession(StompCommand.DISCONNECT, "sess-disconnect-1"),
                "sess-disconnect-1",
                CloseStatus.NORMAL);

        sessionLogger.onDisconnect(event);

        assertThat(loggedMessages())
                .singleElement()
                .satisfies(log -> assertThat(log.getFormattedMessage())
                        .contains("disconnected")
                        .contains("sess-disconnect-1"));
    }
}
