package com.mktplace.commons.web;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CursorCodecTest {

    @Test
    void idaEVolta() {
        Instant at = Instant.parse("2026-09-14T13:45:00.120Z");
        UUID id = UUID.randomUUID();
        String cursor = CursorCodec.encode(at, id);
        assertThat(cursor).doesNotContain("|", "=");
        CursorCodec.Cursor decoded = CursorCodec.decode(cursor);
        assertThat(decoded.at()).isEqualTo(at);
        assertThat(decoded.id()).isEqualTo(id);
    }

    @Test
    void cursorInvalidoVira400() {
        assertThatThrownBy(() -> CursorCodec.decode("nao-e-base64-valido!!"))
                .isInstanceOf(BadRequestException.class)
                .satisfies(e -> assertThat(((ApiException) e).code()).isEqualTo("invalid-cursor"));
        assertThatThrownBy(() -> CursorCodec.decode(java.util.Base64.getUrlEncoder().encodeToString("sem-separador".getBytes())))
                .isInstanceOf(BadRequestException.class);
    }
}
