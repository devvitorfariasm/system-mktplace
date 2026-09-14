package com.mktplace.commons.util;

import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class UuidV7Test {

    @Test
    void temVersao7EVarianteRfc() {
        UUID id = UuidV7.generate();
        assertThat(id.version()).isEqualTo(7);
        assertThat(id.variant()).isEqualTo(2);
    }

    @Test
    void embuteOInstanteEOrdenaPorTempo() {
        UUID older = UuidV7.generate(1_700_000_000_000L);
        UUID newer = UuidV7.generate(1_700_000_000_001L);
        assertThat(UuidV7.timestampMillis(older)).isEqualTo(1_700_000_000_000L);
        assertThat(older.toString().compareTo(newer.toString())).isNegative();
    }

    @Test
    void naoRepete() {
        assertThat(UuidV7.generate()).isNotEqualTo(UuidV7.generate());
    }
}
