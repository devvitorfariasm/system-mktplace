package com.mktplace.payment;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/** Simulação do gateway de pagamento: taxa de aprovação e latência artificial. */
@ConfigurationProperties("mktplace.payment")
public record PaymentProperties(
        @DefaultValue("0.8") double approvalRate,
        @DefaultValue("1000") long latencyMinMs,
        @DefaultValue("3000") long latencyMaxMs) {
}
