package com.mktplace.audit;

import com.mktplace.commons.audit.AuditRecord;
import com.mktplace.commons.events.Topics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;

/** Materializa event-audit-log em audit_events. Idempotente pelo recordId (sem processed_events). */
@Component
@Profile("audit")
public class AuditCollector {

    private static final Logger log = LoggerFactory.getLogger(AuditCollector.class);

    private final AuditEventStore store;
    private final JsonMapper json;

    public AuditCollector(AuditEventStore store, JsonMapper json) {
        this.store = store;
        this.json = json;
    }

    @KafkaListener(id = "audit-collector", topics = Topics.AUDIT_LOG, groupId = "audit-collector")
    public void onAuditRecord(String payload) {
        AuditRecord record = json.readValue(payload, AuditRecord.class);
        if (store.insert(record)) {
            log.debug("audit.stored kind={} eventType={} eventId={} group={} status={}", record.kind(), record.eventType(),
                    record.eventId(), record.consumerGroup(), record.status());
        }
    }
}
