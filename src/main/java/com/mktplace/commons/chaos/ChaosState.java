package com.mktplace.commons.chaos;

import org.jspecify.annotations.Nullable;

import java.util.concurrent.atomic.AtomicInteger;

/** Contadores das injeções. Decrementados por tentativa de entrega, não por mensagem distinta. */
public class ChaosState {

    private final AtomicInteger failRemaining = new AtomicInteger();
    private volatile ChaosMode failMode = ChaosMode.TRANSIENT;
    private final AtomicInteger slowRemaining = new AtomicInteger();
    private volatile long slowMs;

    public void failNext(int count, ChaosMode mode) {
        failMode = mode;
        failRemaining.set(Math.max(0, count));
    }

    public void slow(long ms, int count) {
        slowMs = Math.max(0, ms);
        slowRemaining.set(Math.max(0, count));
    }

    /** @return o modo da falha a injetar nesta tentativa, ou null se não há falha pendente. */
    public @Nullable ChaosMode takeFailure() {
        int remaining = failRemaining.getAndUpdate(n -> n > 0 ? n - 1 : 0);
        return remaining > 0 ? failMode : null;
    }

    /** @return ms de latência a injetar nesta tentativa (0 se nenhuma). */
    public long takeSlow() {
        int remaining = slowRemaining.getAndUpdate(n -> n > 0 ? n - 1 : 0);
        return remaining > 0 ? slowMs : 0;
    }

    public void clear() {
        failRemaining.set(0);
        slowRemaining.set(0);
        slowMs = 0;
    }

    public int failRemaining() {
        return failRemaining.get();
    }

    public ChaosMode failMode() {
        return failMode;
    }

    public int slowRemaining() {
        return slowRemaining.get();
    }

    public long slowMs() {
        return slowMs;
    }
}
