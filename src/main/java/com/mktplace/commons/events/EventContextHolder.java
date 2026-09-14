package com.mktplace.commons.events;

import org.slf4j.MDC;

/**
 * Contexto da cadeia por thread. Preenchido pelo filtro HTTP (correlationId da requisição) e pelo
 * interceptor de consumo (correlationId + eventId do registro consumido). Limpo sempre em finally.
 */
public final class EventContextHolder {

    public static final String MDC_CORRELATION_ID = "correlationId";

    private static final ThreadLocal<EventContext> CURRENT = new ThreadLocal<>();

    private EventContextHolder() {
    }

    public static EventContext get() {
        EventContext context = CURRENT.get();
        return context == null ? EventContext.EMPTY : context;
    }

    public static void set(EventContext context) {
        CURRENT.set(context);
        if (context.correlationId() != null) {
            MDC.put(MDC_CORRELATION_ID, context.correlationId());
        }
    }

    public static void clear() {
        CURRENT.remove();
        MDC.remove(MDC_CORRELATION_ID);
    }
}
