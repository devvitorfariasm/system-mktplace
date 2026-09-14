package com.mktplace.commons.audit;

import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.LongAdder;

/**
 * Contadores locais do listener: mensagens em processamento, total processado/falho e
 * throughput dos últimos 60 s (anel de buckets de 1 s). Alimenta GET /admin/consumer.
 */
@Component
public class ListenerStats {

    private static final int WINDOW_SECONDS = 60;

    private final AtomicInteger inFlight = new AtomicInteger();
    private final LongAdder processed = new LongAdder();
    private final LongAdder failed = new LongAdder();
    private final AtomicLongArray buckets = new AtomicLongArray(WINDOW_SECONDS);
    private final AtomicLongArray bucketEpochSecond = new AtomicLongArray(WINDOW_SECONDS);

    public void started() {
        inFlight.incrementAndGet();
    }

    public void finished(boolean success) {
        inFlight.decrementAndGet();
        (success ? processed : failed).increment();
        long now = System.currentTimeMillis() / 1000;
        int index = (int) (now % WINDOW_SECONDS);
        if (bucketEpochSecond.get(index) != now) {
            bucketEpochSecond.set(index, now);
            buckets.set(index, 0);
        }
        buckets.incrementAndGet(index);
    }

    public int inFlight() {
        return inFlight.get();
    }

    public long processed() {
        return processed.sum();
    }

    public long failed() {
        return failed.sum();
    }

    /** Mensagens por segundo nos últimos 60 s (tentativas, com sucesso ou não). */
    public double throughputPerSecond() {
        long now = System.currentTimeMillis() / 1000;
        long total = 0;
        for (int i = 0; i < WINDOW_SECONDS; i++) {
            if (now - bucketEpochSecond.get(i) < WINDOW_SECONDS) {
                total += buckets.get(i);
            }
        }
        return total / (double) WINDOW_SECONDS;
    }
}
