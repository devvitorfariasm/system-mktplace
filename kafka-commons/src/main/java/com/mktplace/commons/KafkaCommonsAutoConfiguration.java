package com.mktplace.commons;

import com.mktplace.commons.config.MktplaceProperties;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.ComponentScan.Filter;
import org.springframework.context.annotation.FilterType;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;

import java.time.Clock;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Ponto de entrada da biblioteca. Registrada em META-INF/spring/...AutoConfiguration.imports,
 * então basta depender do módulo: nenhum serviço precisa importar nada.
 */
@AutoConfiguration
@EnableConfigurationProperties(MktplaceProperties.class)
@ComponentScan(
        basePackageClasses = KafkaCommonsAutoConfiguration.class,
        excludeFilters = @Filter(type = FilterType.ASSIGNABLE_TYPE, classes = KafkaCommonsAutoConfiguration.class))
public class KafkaCommonsAutoConfiguration {

    /** Relógio único e substituível em testes: todo timestamp de evento/auditoria passa por ele. */
    @Bean
    @ConditionalOnMissingBean
    Clock clock() {
        return Clock.systemUTC();
    }

    /** Continuações de envio Kafka (auditoria, log) fora da thread de rede do producer. */
    @Bean(name = "kafkaCallbackExecutor", destroyMethod = "close")
    ExecutorService kafkaCallbackExecutor() {
        return Executors.newThreadPerTaskExecutor(Thread.ofVirtual().name("kafka-callback-", 0).factory());
    }
}
