package com.mktplace.commons.config;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Clock;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/** Beans transversais do pacote commons. */
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(MktplaceProperties.class)
public class CommonsConfig {

    /** Relógio único e substituível em testes: todo timestamp de evento/auditoria passa por ele. */
    @Bean
    Clock clock() {
        return Clock.systemUTC();
    }

    /** Continuações de envio Kafka (auditoria, log) fora da thread de rede do producer. */
    @Bean(name = "kafkaCallbackExecutor", destroyMethod = "close")
    ExecutorService kafkaCallbackExecutor() {
        return Executors.newThreadPerTaskExecutor(Thread.ofVirtual().name("kafka-callback-", 0).factory());
    }
}
