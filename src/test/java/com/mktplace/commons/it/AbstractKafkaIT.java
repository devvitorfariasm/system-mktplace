package com.mktplace.commons.it;

import com.mktplace.commons.chaos.ChaosService;
import com.mktplace.commons.events.EventPublisher;
import com.mktplace.commons.events.OrderCreated;
import com.mktplace.commons.util.UuidV7;
import org.junit.jupiter.api.AfterEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.test.context.ActiveProfiles;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;

/**
 * Base dos testes de integração: Kafka (KRaft) + Postgres reais via Testcontainers, iniciados uma vez
 * por JVM e compartilhados pelo contexto Spring cacheado (por isso sem @Container: parar/reiniciar
 * entre classes invalidaria o bootstrap-servers do contexto). Sobe a aplicação real com o profile "test".
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("test")
public abstract class AbstractKafkaIT {

    @ServiceConnection
    static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:4.1.0");

    @ServiceConnection
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:17-alpine");

    static {
        KAFKA.start();
        POSTGRES.start();
    }

    protected static final Duration TIMEOUT = Duration.ofSeconds(30);

    @Autowired
    protected EventPublisher publisher;
    @Autowired
    protected ChaosService chaos;
    @Autowired
    protected AuditTap audit;
    @Autowired
    protected TestOrderListener listener;

    @AfterEach
    void clearChaos() {
        chaos.clear();
    }

    protected static OrderCreated newOrder() {
        return new OrderCreated(UuidV7.generate(), UUID.randomUUID(),
                List.of(new OrderCreated.Item("SKU-123", 2, 4990)), 9980, "BRL", OffsetDateTime.now());
    }
}
