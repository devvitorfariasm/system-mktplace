package com.mktplace.commons.admin;

import com.mktplace.commons.audit.ListenerStats;
import com.mktplace.commons.chaos.ChaosService;
import com.mktplace.commons.config.MktplaceProperties;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.jspecify.annotations.Nullable;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/** GET /admin/consumer: estado do(s) listener(s) de negócio deste processo. */
@RestController
public class ConsumerAdminController {

    public record ListenerState(@Nullable String listenerId, @Nullable String groupId, String state,
                                List<String> assignedPartitions, Map<String, Long> committedOffsets) {
    }

    public record ConsumerState(String service, List<ListenerState> listeners, int inFlight, long processed,
                                long failed, double throughputPerSecond) {
    }

    private final ChaosService chaos;
    private final ListenerStats stats;
    private final AdminClient admin;
    private final MktplaceProperties props;

    public ConsumerAdminController(ChaosService chaos, ListenerStats stats, AdminClient admin, MktplaceProperties props) {
        this.chaos = chaos;
        this.stats = stats;
        this.admin = admin;
        this.props = props;
    }

    @GetMapping("/admin/consumer")
    public ConsumerState state() {
        List<ListenerState> listeners = chaos.businessContainers().stream().map(container -> describe(container)).toList();
        return new ConsumerState(props.serviceName(), listeners, stats.inFlight(), stats.processed(), stats.failed(),
                stats.throughputPerSecond());
    }

    private ListenerState describe(MessageListenerContainer container) {
        String state = !container.isRunning() ? "STOPPED"
                : (container.isContainerPaused() || container.isPauseRequested()) ? "PAUSED" : "RUNNING";
        Collection<TopicPartition> assigned = container.getAssignedPartitions();
        List<String> partitions = assigned == null ? List.of()
                : assigned.stream().map(tp -> tp.toString()).sorted().toList();
        return new ListenerState(container.getListenerId(), container.getGroupId(), state, partitions,
                committedOffsets(container.getGroupId()));
    }

    private Map<String, Long> committedOffsets(@Nullable String groupId) {
        Map<String, Long> offsets = new TreeMap<>();
        if (groupId == null) {
            return offsets;
        }
        try {
            Map<TopicPartition, OffsetAndMetadata> committed = admin.listConsumerGroupOffsets(groupId)
                    .partitionsToOffsetAndMetadata().get(5, TimeUnit.SECONDS);
            committed.forEach((tp, om) -> offsets.put(tp.toString(), om == null ? null : om.offset()));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException | TimeoutException e) {
            offsets.put("error", -1L);
        }
        return offsets;
    }
}
