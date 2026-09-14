package com.mktplace.commons.chaos;

import java.util.List;

/** Estado das injeções ativas (GET /admin/chaos/status). */
public record ChaosStatus(
        boolean paused,
        List<String> pausedListeners,
        int failNextRemaining,
        ChaosMode failMode,
        int slowRemaining,
        long slowMs) {
}
