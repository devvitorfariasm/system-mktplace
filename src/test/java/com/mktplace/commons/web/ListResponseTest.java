package com.mktplace.commons.web;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class ListResponseTest {

    @Test
    void comSobraHaProximaPaginaECursorDoUltimoDevolvido() {
        ListResponse<String> page = ListResponse.paged(List.of("a", "b", "c"), 2, s -> "cur-" + s, 10L);
        assertThat(page.data()).containsExactly("a", "b");
        assertThat(page.page().hasMore()).isTrue();
        assertThat(page.page().nextCursor()).isEqualTo("cur-b");
        assertThat(page.meta().totalEstimate()).isEqualTo(10L);
    }

    @Test
    void semSobraNaoHaCursor() {
        ListResponse<String> page = ListResponse.paged(List.of("a"), 2, s -> "cur-" + s, null);
        assertThat(page.page().hasMore()).isFalse();
        assertThat(page.page().nextCursor()).isNull();
    }
}
