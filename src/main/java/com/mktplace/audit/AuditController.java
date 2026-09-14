package com.mktplace.audit;

import org.springframework.context.annotation.Profile;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

/** Rotas do audit-service na fatia vertical; o restante (lineage, consumers, lag...) entra na fase 3. */
@RestController
@RequestMapping("/api/v1/audit")
@Profile("audit")
public class AuditController {

    private final TimelineService timeline;

    public AuditController(TimelineService timeline) {
        this.timeline = timeline;
    }

    @GetMapping("/orders/{orderId}/timeline")
    public TimelineDtos.Timeline timeline(@PathVariable UUID orderId) {
        return timeline.build(orderId);
    }
}
