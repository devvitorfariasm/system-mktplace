package com.mktplace.commons.web;

import org.jspecify.annotations.Nullable;

import java.util.List;
import java.util.function.Function;

/** Envelope padrão de listagem: data + page (cursor) + meta. */
public record ListResponse<T>(List<T> data, Page page, Meta meta) {

    public record Page(int limit, @Nullable String nextCursor, boolean hasMore) {
    }

    public record Meta(@Nullable Long totalEstimate) {
    }

    /** Recebe limit+1 itens: se sobrou um, há próxima página e o cursor aponta para o último devolvido. */
    public static <T> ListResponse<T> paged(List<T> fetched, int limit, Function<T, String> cursorOf, @Nullable Long totalEstimate) {
        boolean hasMore = fetched.size() > limit;
        List<T> data = hasMore ? fetched.subList(0, limit) : fetched;
        String next = hasMore ? cursorOf.apply(data.getLast()) : null;
        return new ListResponse<>(List.copyOf(data), new Page(limit, next, hasMore), new Meta(totalEstimate));
    }

    public static <T> ListResponse<T> unpaged(List<T> data, @Nullable Long totalEstimate) {
        return new ListResponse<>(List.copyOf(data), new Page(data.size(), null, false), new Meta(totalEstimate));
    }
}
