package com.mktplace.commons.events;

import com.mktplace.commons.util.UuidV7;
import org.jspecify.annotations.Nullable;

/**
 * Metadados da cadeia em curso na thread: correlationId (a cadeia inteira) e causationId
 * (o evento que está sendo processado e que causará os próximos).
 */
public record EventContext(@Nullable String correlationId, @Nullable String causationId) {

    public static final EventContext EMPTY = new EventContext(null, null);

    public String correlationIdOrNew() {
        return correlationId != null ? correlationId : UuidV7.generate().toString();
    }
}
