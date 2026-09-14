package com.mktplace.commons.events;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.jspecify.annotations.Nullable;
import org.springframework.kafka.support.KafkaHeaders;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * Leitura de headers Kafka. A maioria é texto UTF-8; o Spring Kafka grava alguns como int/long binários
 * (tentativa de entrega, partição/offset originais na DLT), que são decodificados pelo nome.
 */
public final class HeaderCodec {

    private static final Set<String> INT_HEADERS = Set.of(
            KafkaHeaders.DELIVERY_ATTEMPT, KafkaHeaders.DLT_ORIGINAL_PARTITION);
    private static final Set<String> LONG_HEADERS = Set.of(
            KafkaHeaders.DLT_ORIGINAL_OFFSET, KafkaHeaders.DLT_ORIGINAL_TIMESTAMP);

    private HeaderCodec() {
    }

    public static @Nullable String string(Headers headers, String name) {
        Header header = headers.lastHeader(name);
        return header == null ? null : decode(header);
    }

    public static String require(Headers headers, String name) {
        String value = string(headers, name);
        if (value == null || value.isBlank()) {
            throw new com.mktplace.commons.consumer.MalformedEventException("Header obrigatório ausente: " + name);
        }
        return value;
    }

    public static @Nullable Integer intValue(Headers headers, String name) {
        String value = string(headers, name);
        return value == null ? null : Integer.valueOf(value);
    }

    public static @Nullable Long longValue(Headers headers, String name) {
        String value = string(headers, name);
        return value == null ? null : Long.valueOf(value);
    }

    /** Tentativa de entrega (1 na primeira). O header só existe quando o container habilita deliveryAttemptHeader. */
    public static int deliveryAttempt(Headers headers) {
        Integer attempt = intValue(headers, KafkaHeaders.DELIVERY_ATTEMPT);
        return attempt == null ? 1 : attempt;
    }

    public static Map<String, String> toMap(Headers headers) {
        Map<String, String> map = new LinkedHashMap<>();
        for (Header header : headers) {
            map.put(header.key(), decode(header));
        }
        return map;
    }

    public static String decode(Header header) {
        byte[] value = header.value();
        if (value == null) {
            return "";
        }
        if (INT_HEADERS.contains(header.key()) && value.length == Integer.BYTES) {
            return Integer.toString(ByteBuffer.wrap(value).getInt());
        }
        if (LONG_HEADERS.contains(header.key()) && value.length == Long.BYTES) {
            return Long.toString(ByteBuffer.wrap(value).getLong());
        }
        String text = new String(value, StandardCharsets.UTF_8);
        return isPrintable(text) ? text : "base64:" + Base64.getEncoder().encodeToString(value);
    }

    private static boolean isPrintable(String text) {
        return text.chars().noneMatch(c -> c == 0xFFFD || (c < 0x20 && c != '\n' && c != '\r' && c != '\t'));
    }
}
