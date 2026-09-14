package com.mktplace.commons.chaos;

import com.mktplace.commons.consumer.PoisonMessageException;
import com.mktplace.commons.consumer.TransientFailureException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * Injeção de falhas para o avaliador ver o Kafka sob estresse. Aplicada pelo AuditRecordInterceptor
 * antes do listener. Pausa/retoma só os listeners de negócio: os internos (id "internal-*", ex.: a
 * materialização da DLT) continuam rodando.
 */
@Service
public class ChaosService {

    public static final String INTERNAL_LISTENER_PREFIX = "internal-";
    private static final Logger log = LoggerFactory.getLogger(ChaosService.class);

    private final ChaosState state = new ChaosState();
    private final KafkaListenerEndpointRegistry registry;

    public ChaosService(KafkaListenerEndpointRegistry registry) {
        this.registry = registry;
    }

    public void beforeProcessing(ConsumerRecord<?, ?> record) {
        long slow = state.takeSlow();
        if (slow > 0) {
            log.warn("chaos.slow ms={} topic={} offset={}", slow, record.topic(), record.offset());
            try {
                Thread.sleep(slow);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        ChaosMode failure = state.takeFailure();
        if (failure == ChaosMode.POISON) {
            log.warn("chaos.fail mode=POISON topic={} offset={}", record.topic(), record.offset());
            throw new PoisonMessageException("chaos: mensagem marcada como POISON");
        }
        if (failure == ChaosMode.TRANSIENT) {
            log.warn("chaos.fail mode=TRANSIENT topic={} offset={}", record.topic(), record.offset());
            throw new TransientFailureException("chaos: falha transitória injetada");
        }
    }

    public ChaosStatus failNext(int count, ChaosMode mode) {
        state.failNext(count, mode);
        return status();
    }

    public ChaosStatus slow(long ms, int count) {
        state.slow(ms, count);
        return status();
    }

    public ChaosStatus pause() {
        List<MessageListenerContainer> containers = businessContainers();
        containers.forEach(container -> container.pause());
        log.warn("chaos.pause listeners={}", containers.stream().map(container -> container.getListenerId()).toList());
        return status();
    }

    public ChaosStatus resume() {
        businessContainers().forEach(container -> container.resume());
        log.warn("chaos.resume");
        return status();
    }

    /** Sai do processo após 500 ms para observar rebalance e o restart do container. */
    public void crash() {
        log.error("chaos.crash: encerrando o processo em 500 ms");
        Thread.ofPlatform().daemon(false).start(() -> {
            try {
                Thread.sleep(500);
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
            System.exit(1);
        });
    }

    public ChaosStatus clear() {
        state.clear();
        resume();
        return status();
    }

    public ChaosStatus status() {
        List<String> paused = businessContainers().stream()
                .filter(c -> c.isPauseRequested() || c.isContainerPaused())
                .map(container -> container.getListenerId())
                .toList();
        return new ChaosStatus(!paused.isEmpty(), paused, state.failRemaining(), state.failMode(),
                state.slowRemaining(), state.slowMs());
    }

    public List<MessageListenerContainer> businessContainers() {
        return registry.getListenerContainers().stream()
                .filter(c -> c.getListenerId() == null || !c.getListenerId().startsWith(INTERNAL_LISTENER_PREFIX))
                .toList();
    }

    public boolean ownsGroup(String groupId) {
        return businessContainers().stream().anyMatch(c -> groupId.equals(c.getGroupId()));
    }
}
