package com.mktplace.commons.events;

/** Nomes canônicos dos eventos (valor do header "eventType"). */
public final class EventTypes {

    public static final String ORDER_CREATED = "OrderCreated";
    public static final String ORDER_CANCELLED = "OrderCancelled";
    public static final String PAYMENT_APPROVED = "PaymentApproved";
    public static final String PAYMENT_REJECTED = "PaymentRejected";
    public static final String PAYMENT_REFUNDED = "PaymentRefunded";
    public static final String STOCK_RESERVED = "StockReserved";
    public static final String STOCK_UNAVAILABLE = "StockUnavailable";
    public static final String STOCK_RELEASED = "StockReleased";
    public static final String STOCK_ADJUSTED = "StockAdjusted";
    public static final String NOTIFICATION_SENT = "NotificationSent";
    /** Registrado na auditoria quando um operador descarta uma mensagem da DLQ. */
    public static final String DLQ_MESSAGE_DISCARDED = "DlqMessageDiscarded";

    private EventTypes() {
    }
}
