package com.mktplace.commons.dlq;

import com.mktplace.commons.events.PublishedEvent;
import com.mktplace.commons.web.ListResponse;
import com.mktplace.commons.web.NotFoundException;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;
import java.util.UUID;

/** Rotas /admin/dlq e /admin/jobs (protegidas por X-Admin-Token). */
@RestController
@RequestMapping("/admin")
@Validated
public class DlqAdminController {

    public record DiscardRequest(@NotBlank(message = "Motivo é obrigatório") String reason) {
    }

    private final DlqStore store;
    private final DlqReprocessService reprocess;
    private final ReprocessJobs jobs;

    public DlqAdminController(DlqStore store, DlqReprocessService reprocess, ReprocessJobs jobs) {
        this.store = store;
        this.reprocess = reprocess;
        this.jobs = jobs;
    }

    @GetMapping("/dlq")
    public ListResponse<DlqMessage> list(@RequestParam(required = false) String originalTopic,
                                         @RequestParam(defaultValue = "PENDING") DlqStatus status,
                                         @RequestParam(defaultValue = "50") int limit) {
        var messages = store.findAll(status, originalTopic, Math.clamp(limit, 1, 500)).stream()
                .map(message -> message.summary()).toList();
        return ListResponse.unpaged(messages, store.countPending());
    }

    @GetMapping("/dlq/{messageId}")
    public DlqMessage get(@PathVariable UUID messageId) {
        return store.find(messageId)
                .orElseThrow(() -> new NotFoundException("dlq-message-not-found", "Mensagem de DLQ não encontrada: " + messageId));
    }

    @PostMapping("/dlq/{messageId}/reprocess")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public PublishedEvent reprocess(@PathVariable UUID messageId) {
        return reprocess.reprocess(messageId);
    }

    @PostMapping("/dlq/reprocess-all")
    public ResponseEntity<Map<String, Object>> reprocessAll(@RequestParam(required = false) String originalTopic) {
        UUID jobId = reprocess.reprocessAll(originalTopic);
        return ResponseEntity.status(HttpStatus.ACCEPTED)
                .body(Map.of("jobId", jobId, "status", "/admin/jobs/" + jobId));
    }

    @DeleteMapping("/dlq/{messageId}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void discard(@PathVariable UUID messageId, @Valid @RequestBody DiscardRequest request) {
        reprocess.discard(messageId, request.reason());
    }

    @GetMapping("/jobs/{jobId}")
    public ReprocessJobs.JobStatus job(@PathVariable UUID jobId) {
        return jobs.find(jobId).orElseThrow(() -> new NotFoundException("job-not-found", "Job não encontrado: " + jobId));
    }
}
