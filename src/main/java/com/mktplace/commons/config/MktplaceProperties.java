package com.mktplace.commons.config;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;
import org.springframework.validation.annotation.Validated;

import java.time.ZoneId;

/**
 * Configuração tipada de todo serviço (prefixo "mktplace").
 *
 * @param serviceName nome do serviço: vira header "producer", nome do consumer group padrão e tag de métricas
 * @param adminToken  valor exigido no header X-Admin-Token das rotas /admin/**
 * @param zone        fuso usado para formatar datas com offset nas respostas e headers
 */
@ConfigurationProperties("mktplace")
@Validated
public record MktplaceProperties(
        @NotBlank String serviceName,
        @NotBlank String adminToken,
        @DefaultValue("America/Sao_Paulo") ZoneId zone,
        @DefaultValue Kafka kafka) {

    /**
     * @param maxAttempts       entregas totais por mensagem (1 + retries); 4 = 3 retries
     * @param backoffInitialMs  espera antes do primeiro retry
     * @param backoffMultiplier fator exponencial entre retries
     * @param backoffMaxMs      teto da espera
     * @param partitions        partições dos tópicos de negócio (mínimo 3 pelo desafio)
     * @param replication       fator de replicação (1 no cluster local de um nó)
     */
    public record Kafka(
            @DefaultValue("4") @Min(1) int maxAttempts,
            @DefaultValue("1000") long backoffInitialMs,
            @DefaultValue("2.0") double backoffMultiplier,
            @DefaultValue("10000") long backoffMaxMs,
            @DefaultValue("3") @Min(3) int partitions,
            @DefaultValue("1") short replication) {
    }
}
