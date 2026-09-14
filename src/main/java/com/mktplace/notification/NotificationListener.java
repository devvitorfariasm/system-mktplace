package com.mktplace.notification;

import com.mktplace.commons.consumer.IdempotencyGuard;
import com.mktplace.commons.events.EventHeaders;
import com.mktplace.commons.events.HeaderCodec;
import com.mktplace.commons.events.NotificationSent;
import com.mktplace.commons.events.Topics;
import com.mktplace.commons.outbox.OutboxPublisher;
import com.mktplace.commons.util.UuidV7;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.time.Clock;
import java.time.OffsetDateTime;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Consome os eventos terminais, renderiza o template do tipo de evento e registra a notificação. */
@Component
@Profile("notification")
public class NotificationListener {

    private static final Logger log = LoggerFactory.getLogger(NotificationListener.class);
    private static final Pattern PLACEHOLDER = Pattern.compile("\\{\\{(\\w+)}}");

    private final NotificationRepository notifications;
    private final JdbcClient jdbc;
    private final JsonMapper json;
    private final IdempotencyGuard guard;
    private final OutboxPublisher outbox;
    private final Clock clock;

    public NotificationListener(NotificationRepository notifications, JdbcClient jdbc, JsonMapper json, IdempotencyGuard guard,
                                OutboxPublisher outbox, Clock clock) {
        this.notifications = notifications;
        this.jdbc = jdbc;
        this.json = json;
        this.guard = guard;
        this.outbox = outbox;
        this.clock = clock;
    }

    @KafkaListener(id = "notification-service", groupId = "notification-service-v1",
            topics = {Topics.INVENTORY_RESERVED, Topics.PAYMENTS_REJECTED, Topics.INVENTORY_UNAVAILABLE})
    public void onTerminalEvent(ConsumerRecord<String, String> record, @Header(KafkaHeaders.GROUP_ID) String groupId) {
        String eventType = HeaderCodec.require(record.headers(), EventHeaders.EVENT_TYPE);
        String eventId = HeaderCodec.require(record.headers(), EventHeaders.EVENT_ID);
        UUID orderId = UUID.fromString(HeaderCodec.require(record.headers(), EventHeaders.ORDER_ID));
        JsonNode payload = json.readTree(record.value());
        guard.runOnce(record, groupId, () -> {
            var template = jdbc.sql("SELECT subject, body FROM notification_templates WHERE event_type = :type")
                    .param("type", eventType)
                    .query((rs, i) -> Map.of("subject", rs.getString("subject"), "body", rs.getString("body")))
                    .optional()
                    .orElse(Map.of("subject", "Atualização do pedido {{orderId}}", "body", "Evento {{eventType}} no pedido {{orderId}}."));
            var vars = Map.of("orderId", orderId.toString(), "eventType", eventType,
                    "total", payload.has("amount") ? payload.get("amount").asString() : "",
                    "reason", payload.has("reason") ? payload.get("reason").asString() : "");
            var now = OffsetDateTime.now(clock);
            var notification = new Notification(UuidV7.generate(), orderId, null, "EMAIL", "SENT", eventType, eventId,
                    render(template.get("subject"), vars), render(template.get("body"), vars), now);
            notifications.save(notification);
            outbox.enqueue(new NotificationSent(orderId, notification.getId(), null, "EMAIL", eventType, now));
            log.info("notification.sent orderId={} notificationId={} trigger={}", orderId, notification.getId(), eventType);
        });
    }

    static String render(String template, Map<String, String> vars) {
        Matcher m = PLACEHOLDER.matcher(template);
        StringBuilder out = new StringBuilder();
        while (m.find()) {
            m.appendReplacement(out, Matcher.quoteReplacement(vars.getOrDefault(m.group(1), "")));
        }
        m.appendTail(out);
        return out.toString();
    }
}
