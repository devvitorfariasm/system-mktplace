package com.mktplace.commons.util;

import java.security.SecureRandom;
import java.util.UUID;

/**
 * UUID v7 (RFC 9562): 48 bits de epoch em ms + 74 bits aleatórios. Ordenável por tempo,
 * o que torna cursores (producedAt, id) e índices B-tree mais eficientes que UUID v4.
 */
public final class UuidV7 {

    private static final SecureRandom RANDOM = new SecureRandom();

    private UuidV7() {
    }

    public static UUID generate() {
        return generate(System.currentTimeMillis());
    }

    static UUID generate(long epochMillis) {
        long msb = (epochMillis << 16) | 0x7000L | (RANDOM.nextInt() & 0x0FFFL);
        long lsb = (RANDOM.nextLong() & 0x3FFFFFFFFFFFFFFFL) | 0x8000000000000000L;
        return new UUID(msb, lsb);
    }

    /** Instante embutido no UUID (útil para depuração e para validar cursores). */
    public static long timestampMillis(UUID uuid) {
        return uuid.getMostSignificantBits() >>> 16;
    }
}
