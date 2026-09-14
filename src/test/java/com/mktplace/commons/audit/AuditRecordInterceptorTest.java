package com.mktplace.commons.audit;

import com.mktplace.commons.config.MktplaceProperties;
import com.mktplace.commons.consumer.PoisonMessageException;
import com.mktplace.commons.consumer.RetryPolicy;
import com.mktplace.commons.consumer.TransientFailureException;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.listener.ListenerExecutionFailedException;

import java.time.Clock;
import java.time.ZoneId;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

class AuditRecordInterceptorTest {

    private final MktplaceProperties props = new MktplaceProperties("svc", "tok", ZoneId.of("UTC"),
            new MktplaceProperties.Kafka(4, 100, 2.0, 500, 3, (short) 1));
    private final AuditRecordInterceptor interceptor = new AuditRecordInterceptor(
            mock(AuditPublisher.class), mock(com.mktplace.commons.chaos.ChaosService.class), new ListenerStats(),
            props, Clock.systemUTC());

    @Test
    void transitoriaComTentativasRestantesEhRetry() {
        assertThat(interceptor.classify(new TransientFailureException("x"), 1)).isEqualTo(ConsumeStatus.RETRY);
        assertThat(interceptor.classify(new TransientFailureException("x"), 3)).isEqualTo(ConsumeStatus.RETRY);
    }

    @Test
    void ultimaTentativaEhDlq() {
        assertThat(interceptor.classify(new TransientFailureException("x"), 4)).isEqualTo(ConsumeStatus.DLQ);
    }

    @Test
    void naoRetryableEhDlqMesmoNaPrimeiraTentativaEMesmoEmbrulhada() {
        assertThat(interceptor.classify(new PoisonMessageException("x"), 1)).isEqualTo(ConsumeStatus.DLQ);
        var wrapped = new ListenerExecutionFailedException("listener falhou", new PoisonMessageException("x"));
        assertThat(interceptor.classify(wrapped, 1)).isEqualTo(ConsumeStatus.DLQ);
        assertThat(RetryPolicy.rootCause(wrapped)).isInstanceOf(PoisonMessageException.class);
    }
}
