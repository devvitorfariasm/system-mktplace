package com.mktplace.commons.events;

import java.util.List;
import java.util.Map;

/**
 * Nomes de tópicos. Um tópico por tipo de evento, chave = orderId (ADR 0001).
 * Todo tópico de negócio tem o irmão "<topic>.DLT" com o mesmo número de partições.
 */
public final class Topics {

    public static final String ORDERS_CREATED = "orders.created";
    public static final String ORDERS_CANCELLED = "orders.cancelled";
    public static final String PAYMENTS_APPROVED = "payments.approved";
    public static final String PAYMENTS_REJECTED = "payments.rejected";
    public static final String PAYMENTS_REFUNDED = "payments.refunded";
    public static final String INVENTORY_RESERVED = "inventory.reserved";
    public static final String INVENTORY_UNAVAILABLE = "inventory.unavailable";
    public static final String INVENTORY_RELEASED = "inventory.released";
    public static final String INVENTORY_ADJUSTED = "inventory.adjusted";
    public static final String NOTIFICATIONS_SENT = "notifications.sent";

    /** Tópico de auditoria: todo produtor e todo consumidor registra aqui o que fez com cada evento. */
    public static final String AUDIT_LOG = "event-audit-log";
    public static final String DLT_SUFFIX = ".DLT";

    public static final List<String> BUSINESS = List.of(
            ORDERS_CREATED, ORDERS_CANCELLED,
            PAYMENTS_APPROVED, PAYMENTS_REJECTED, PAYMENTS_REFUNDED,
            INVENTORY_RESERVED, INVENTORY_UNAVAILABLE, INVENTORY_RELEASED, INVENTORY_ADJUSTED,
            NOTIFICATIONS_SENT);

    private static final Map<String, String> TOPIC_BY_EVENT_TYPE = Map.ofEntries(
            Map.entry(EventTypes.ORDER_CREATED, ORDERS_CREATED),
            Map.entry(EventTypes.ORDER_CANCELLED, ORDERS_CANCELLED),
            Map.entry(EventTypes.PAYMENT_APPROVED, PAYMENTS_APPROVED),
            Map.entry(EventTypes.PAYMENT_REJECTED, PAYMENTS_REJECTED),
            Map.entry(EventTypes.PAYMENT_REFUNDED, PAYMENTS_REFUNDED),
            Map.entry(EventTypes.STOCK_RESERVED, INVENTORY_RESERVED),
            Map.entry(EventTypes.STOCK_UNAVAILABLE, INVENTORY_UNAVAILABLE),
            Map.entry(EventTypes.STOCK_RELEASED, INVENTORY_RELEASED),
            Map.entry(EventTypes.STOCK_ADJUSTED, INVENTORY_ADJUSTED),
            Map.entry(EventTypes.NOTIFICATION_SENT, NOTIFICATIONS_SENT));

    private Topics() {
    }

    public static String forEventType(String eventType) {
        String topic = TOPIC_BY_EVENT_TYPE.get(eventType);
        if (topic == null) {
            throw new IllegalArgumentException("Tipo de evento sem tópico mapeado: " + eventType);
        }
        return topic;
    }

    public static String dlt(String topic) {
        return topic + DLT_SUFFIX;
    }

    public static boolean isDlt(String topic) {
        return topic != null && topic.endsWith(DLT_SUFFIX);
    }

    /** Auditamos tópicos de negócio; nunca a auditoria nem as DLTs (evitaria loop e ruído). */
    public static boolean isAuditable(String topic) {
        return topic != null && !AUDIT_LOG.equals(topic) && !isDlt(topic);
    }
}
