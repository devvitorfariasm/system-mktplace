package com.mktplace.commons.it;

import com.mktplace.commons.audit.AuditKind;
import com.mktplace.commons.audit.AuditRecord;
import com.mktplace.commons.chaos.ChaosService;
import com.mktplace.commons.events.Topics;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/** Lê event-audit-log como o audit-service fará, guardando tudo em memória para as asserções. */
@Component
public class AuditTap {

    private final JsonMapper mapper;
    private final List<AuditRecord> records = new CopyOnWriteArrayList<>();

    public AuditTap(JsonMapper mapper) {
        this.mapper = mapper;
    }

    @KafkaListener(id = ChaosService.INTERNAL_LISTENER_PREFIX + "audit-tap", topics = Topics.AUDIT_LOG, groupId = "test-audit-tap")
    public void onAudit(String payload) {
        records.add(mapper.readValue(payload, AuditRecord.class));
    }

    public List<AuditRecord> forEvent(String eventId, AuditKind kind) {
        return records.stream()
                .filter(r -> eventId.equals(r.eventId()) && r.kind() == kind)
                .sorted((a, b) -> a.recordedAt().compareTo(b.recordedAt()))
                .toList();
    }

    public List<AuditRecord> all() {
        return List.copyOf(records);
    }
}
