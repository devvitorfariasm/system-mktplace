package com.mktplace.commons.consumer;

import com.mktplace.commons.config.MktplaceProperties;
import com.mktplace.commons.events.EventHeaders;
import com.mktplace.commons.events.Topics;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.jspecify.annotations.Nullable;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.config.ContainerCustomizer;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.support.TopicPartitionOffset;
import org.springframework.util.backoff.BackOff;
import org.springframework.util.backoff.ExponentialBackOff;

import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Retry com backoff exponencial seguido de Dead Letter Topic por tópico ("<topic>.DLT", mesma partição).
 * O Spring Boot injeta automaticamente o CommonErrorHandler, o RecordInterceptor e o ContainerCustomizer
 * na fábrica de listeners padrão.
 */
@Configuration(proxyBeanMethods = false)
public class KafkaConsumerConfig {

    @Bean
    CommonErrorHandler kafkaErrorHandler(KafkaTemplate<String, String> template, MktplaceProperties props) {
        var recoverer = new DeadLetterPublishingRecoverer(template,
                (record, error) -> new TopicPartition(Topics.dlt(record.topic()), record.partition()));
        // grupo e tentativa vêm do próprio error handler (ver TrackingErrorHandler), não de headers copiados
        recoverer.addHeadersFunction((record, error) -> {
            var headers = new RecordHeaders();
            TrackingErrorHandler.Recovery recovery = TrackingErrorHandler.CURRENT.get();
            if (recovery != null) {
                if (recovery.groupId() != null) {
                    headers.add(EventHeaders.DLQ_CONSUMER_GROUP, recovery.groupId().getBytes(StandardCharsets.UTF_8));
                }
                headers.add(EventHeaders.DLQ_ATTEMPTS, Integer.toString(recovery.attempt()).getBytes(StandardCharsets.UTF_8));
            }
            return headers;
        });
        var kafka = props.kafka();
        var backOff = new ExponentialBackOff(kafka.backoffInitialMs(), kafka.backoffMultiplier());
        backOff.setMaxInterval(kafka.backoffMaxMs());
        backOff.setMaxAttempts(kafka.maxAttempts() - 1);   // retries = entregas totais - 1
        var handler = new TrackingErrorHandler(recoverer, backOff);
        for (Class<? extends Exception> type : RetryPolicy.NON_RETRYABLE) {
            handler.addNotRetryableExceptions(type);
        }
        handler.setLogLevel(KafkaException.Level.WARN);
        return handler;
    }

    /** Header kafka_deliveryAttempt em cada entrega: é o "attempt" da linha do tempo. */
    @Bean
    ContainerCustomizer<Object, Object, ConcurrentMessageListenerContainer<Object, Object>> deliveryAttemptCustomizer() {
        return container -> container.getContainerProperties().setDeliveryAttemptHeader(true);
    }

    /**
     * DefaultErrorHandler que expõe, na thread, o consumer group do container e o número da tentativa
     * que falhou, para o recoverer gravá-los na DLT (x-dlq-consumer-group, x-dlq-attempts).
     */
    static final class TrackingErrorHandler extends DefaultErrorHandler {

        record Recovery(@Nullable String groupId, int attempt) {
        }

        static final ThreadLocal<Recovery> CURRENT = new ThreadLocal<>();

        TrackingErrorHandler(ConsumerRecordRecoverer recoverer, BackOff backOff) {
            super(recoverer, backOff);
        }

        @Override
        public boolean handleOne(Exception thrownException, ConsumerRecord<?, ?> record, Consumer<?, ?> consumer,
                                 MessageListenerContainer container) {
            track(record, container);
            try {
                return super.handleOne(thrownException, record, consumer, container);
            } finally {
                CURRENT.remove();
            }
        }

        @Override
        public void handleRemaining(Exception thrownException, List<ConsumerRecord<?, ?>> records, Consumer<?, ?> consumer,
                                    MessageListenerContainer container) {
            if (!records.isEmpty()) {
                track(records.getFirst(), container);
            }
            try {
                super.handleRemaining(thrownException, records, consumer, container);
            } finally {
                CURRENT.remove();
            }
        }

        private void track(ConsumerRecord<?, ?> record, MessageListenerContainer container) {
            int attempt = deliveryAttempt(new TopicPartitionOffset(record.topic(), record.partition(), record.offset()));
            CURRENT.set(new Recovery(container.getGroupId(), attempt));
        }
    }
}
