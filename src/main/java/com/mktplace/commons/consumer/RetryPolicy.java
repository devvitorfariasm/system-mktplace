package com.mktplace.commons.consumer;

import org.springframework.core.convert.ConversionException;
import org.springframework.messaging.handler.invocation.MethodArgumentResolutionException;
import org.springframework.messaging.converter.MessageConversionException;
import org.springframework.kafka.support.serializer.DeserializationException;

import java.util.List;

/**
 * Lista única de exceções não-retryable, usada pelo DefaultErrorHandler (decide se vai para a DLT)
 * e pelo interceptor de auditoria (classifica a tentativa como RETRY ou DLQ). Manter os dois iguais
 * garante que o que a linha do tempo mostra é o que o Kafka fez.
 */
public final class RetryPolicy {

    public static final List<Class<? extends Exception>> NON_RETRYABLE = List.of(
            NonRetryableEventException.class,
            DeserializationException.class,
            MessageConversionException.class,
            ConversionException.class,
            MethodArgumentResolutionException.class,
            ClassCastException.class);

    private RetryPolicy() {
    }

    public static boolean isNonRetryable(Throwable error) {
        Throwable current = error;
        while (current != null) {
            for (Class<? extends Exception> type : NON_RETRYABLE) {
                if (type.isInstance(current)) {
                    return true;
                }
            }
            current = current.getCause() == current ? null : current.getCause();
        }
        return false;
    }

    public static Throwable rootCause(Throwable error) {
        Throwable current = error;
        while (current.getCause() != null && current.getCause() != current) {
            current = current.getCause();
        }
        return current;
    }
}
