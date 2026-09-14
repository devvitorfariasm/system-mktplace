package com.mktplace.commons.consumer;

import com.mktplace.commons.config.MktplaceProperties;
import com.mktplace.commons.events.Topics;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Tópicos criados via código (auto-create está desligado no broker). Todo serviço declara todos:
 * KafkaAdmin é idempotente e assim qualquer serviço pode subir primeiro.
 */
@Configuration(proxyBeanMethods = false)
public class KafkaTopicsConfig {

    private static final String RETENTION_7D = Long.toString(Duration.ofDays(7).toMillis());

    @Bean
    KafkaAdmin.NewTopics mktplaceTopics(MktplaceProperties props) {
        int partitions = props.kafka().partitions();
        short replication = props.kafka().replication();
        List<NewTopic> topics = new ArrayList<>();
        for (String topic : Topics.BUSINESS) {
            topics.add(TopicBuilder.name(topic).partitions(partitions).replicas(replication)
                    .configs(Map.of(TopicConfig.RETENTION_MS_CONFIG, RETENTION_7D)).build());
            // DLT com as mesmas partições: a mensagem morta preserva a partição de origem
            topics.add(TopicBuilder.name(Topics.dlt(topic)).partitions(partitions).replicas(replication)
                    .configs(Map.of(TopicConfig.RETENTION_MS_CONFIG, RETENTION_7D)).build());
        }
        topics.add(TopicBuilder.name(Topics.AUDIT_LOG).partitions(partitions).replicas(replication)
                .configs(Map.of(TopicConfig.RETENTION_MS_CONFIG, RETENTION_7D)).build());
        return new KafkaAdmin.NewTopics(topics.toArray(NewTopic[]::new));
    }

    /** AdminClient compartilhado: lag, offsets commitados, estado dos grupos (fase 3 e /admin/consumer). */
    @Bean(destroyMethod = "close")
    AdminClient kafkaAdminClient(KafkaAdmin kafkaAdmin) {
        return AdminClient.create(kafkaAdmin.getConfigurationProperties());
    }
}
