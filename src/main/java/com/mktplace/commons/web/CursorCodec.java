package com.mktplace.commons.web;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.Base64;
import java.util.UUID;

/**
 * Cursor opaco baseado em (instante, id): base64url de "epochMicros|uuid". O par é único e ordenável,
 * então a próxima página é "WHERE (at, id) < (:at, :id)" com índice composto, sem OFFSET.
 */
public final class CursorCodec {

    public record Cursor(Instant at, UUID id) {
    }

    private CursorCodec() {
    }

    public static String encode(Instant at, UUID id) {
        String raw = at.toString() + "|" + id;
        return Base64.getUrlEncoder().withoutPadding().encodeToString(raw.getBytes(StandardCharsets.UTF_8));
    }

    public static Cursor decode(String cursor) {
        try {
            String raw = new String(Base64.getUrlDecoder().decode(cursor), StandardCharsets.UTF_8);
            int sep = raw.indexOf('|');
            if (sep < 0) {
                throw new IllegalArgumentException("separador ausente");
            }
            return new Cursor(Instant.parse(raw.substring(0, sep)), UUID.fromString(raw.substring(sep + 1)));
        } catch (IllegalArgumentException | DateTimeParseException e) {
            throw new BadRequestException("invalid-cursor", "Cursor inválido");
        }
    }
}
