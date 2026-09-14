package com.mktplace.commons.events;

import com.mktplace.commons.consumer.MalformedEventException;
import com.mktplace.commons.consumer.UnsupportedSchemaVersionException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.cfg.DateTimeFeature;
import tools.jackson.databind.json.JsonMapper;

import java.nio.charset.StandardCharsets;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class EventCodecTest {

    private final EventCodec codec = new EventCodec(JsonMapper.builder()
            .disable(DateTimeFeature.ADJUST_DATES_TO_CONTEXT_TIME_ZONE).build());

    private static ConsumerRecord<String, String> record(String eventType, String version, String payload) {
        var headers = new RecordHeaders();
        headers.add(EventHeaders.EVENT_TYPE, eventType.getBytes(StandardCharsets.UTF_8));
        if (version != null) {
            headers.add(EventHeaders.SCHEMA_VERSION, version.getBytes(StandardCharsets.UTF_8));
        }
        return new ConsumerRecord<>(Topics.ORDERS_CREATED, 0, 0L, 0L, TimestampType.CREATE_TIME, 0, 0,
                "k", payload, headers, Optional.empty());
    }

    @Test
    void idaEVoltaDeUmEventoV1() {
        OrderCreated event = new OrderCreated(UUID.randomUUID(), UUID.randomUUID(),
                List.of(new OrderCreated.Item("SKU-1", 1, 100)), 100, "BRL", OffsetDateTime.parse("2026-09-14T13:45:00-03:00"));
        String json = codec.encode(event);
        assertThat(json).contains("\"sku\":\"SKU-1\"").doesNotContain("eventType").doesNotContain("\"key\"");

        OrderCreated decoded = codec.decode(record(EventTypes.ORDER_CREATED, "1", json), OrderCreated.class);
        assertThat(decoded).isEqualTo(event);
    }

    @Test
    void camposDesconhecidosSaoIgnoradosEVersaoAusenteVale1() {
        String json = "{\"orderId\":\"%s\",\"total\":5,\"currency\":\"BRL\",\"novoCampo\":true}".formatted(UUID.randomUUID());
        OrderCreated decoded = codec.decode(record(EventTypes.ORDER_CREATED, null, json), OrderCreated.class);
        assertThat(decoded.total()).isEqualTo(5);
    }

    @Test
    void upcasterTransformaV1EmV2() {
        // v2 hipotético: "amount" no lugar de "total"
        codec.registerUpcaster(EventTypes.PAYMENT_APPROVED, 1, node -> {
            node.set("amount", node.remove("total"));
            return node;
        });
        assertThat(codec.currentVersion(EventTypes.PAYMENT_APPROVED)).isEqualTo(2);
        String v1 = "{\"orderId\":\"%s\",\"paymentId\":\"%s\",\"total\":990,\"currency\":\"BRL\"}"
                .formatted(UUID.randomUUID(), UUID.randomUUID());
        PaymentApproved decoded = codec.decode(record(EventTypes.PAYMENT_APPROVED, "1", v1), PaymentApproved.class);
        assertThat(decoded.amount()).isEqualTo(990);
    }

    @Test
    void versaoMaiorQueAConhecidaEhNaoRetryable() {
        assertThatThrownBy(() -> codec.decode(record(EventTypes.ORDER_CREATED, "9", "{}"), OrderCreated.class))
                .isInstanceOf(UnsupportedSchemaVersionException.class);
    }

    @Test
    void jsonQuebradoOuHeaderAusenteEhNaoRetryable() {
        assertThatThrownBy(() -> codec.decode(record(EventTypes.ORDER_CREATED, "1", "{nao json"), OrderCreated.class))
                .isInstanceOf(MalformedEventException.class);
        var semTipo = new ConsumerRecord<>(Topics.ORDERS_CREATED, 0, 0L, "k", "{}");
        assertThatThrownBy(() -> codec.decode(semTipo, OrderCreated.class))
                .isInstanceOf(MalformedEventException.class);
    }
}
