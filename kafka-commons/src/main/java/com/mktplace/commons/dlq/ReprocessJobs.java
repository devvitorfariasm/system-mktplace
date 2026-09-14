package com.mktplace.commons.dlq;

import com.mktplace.commons.util.UuidV7;
import org.jspecify.annotations.Nullable;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.time.OffsetDateTime;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/** Progresso dos reprocessamentos em lote (GET /admin/jobs/{jobId}). Em memória: o job vive com o processo. */
@Component
public class ReprocessJobs {

    public record JobStatus(UUID jobId, @Nullable String originalTopic, String status, int total, int done,
                            int failed, OffsetDateTime startedAt, @Nullable OffsetDateTime finishedAt) {
    }

    public static final class Job {
        final UUID id = UuidV7.generate();
        final @Nullable String originalTopic;
        final int total;
        final AtomicInteger done = new AtomicInteger();
        final AtomicInteger failed = new AtomicInteger();
        final OffsetDateTime startedAt;
        volatile @Nullable OffsetDateTime finishedAt;

        Job(@Nullable String originalTopic, int total, OffsetDateTime startedAt) {
            this.originalTopic = originalTopic;
            this.total = total;
            this.startedAt = startedAt;
        }

        public UUID id() {
            return id;
        }

        public void succeeded() {
            done.incrementAndGet();
        }

        public void failed() {
            failed.incrementAndGet();
        }

        JobStatus snapshot() {
            return new JobStatus(id, originalTopic, finishedAt == null ? "RUNNING" : "DONE", total, done.get(),
                    failed.get(), startedAt, finishedAt);
        }
    }

    private final Map<UUID, Job> jobs = new ConcurrentHashMap<>();
    private final Clock clock;

    public ReprocessJobs(Clock clock) {
        this.clock = clock;
    }

    public Job create(@Nullable String originalTopic, int total) {
        var job = new Job(originalTopic, total, OffsetDateTime.now(clock));
        jobs.put(job.id, job);
        return job;
    }

    public void finish(Job job) {
        job.finishedAt = OffsetDateTime.now(clock);
    }

    public Optional<JobStatus> find(UUID jobId) {
        return Optional.ofNullable(jobs.get(jobId)).map(job -> job.snapshot());
    }
}
