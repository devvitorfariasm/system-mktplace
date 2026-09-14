package com.mktplace.commons.events;

import com.mktplace.commons.consumer.MalformedEventException;
import com.mktplace.commons.consumer.UnsupportedSchemaVersionException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.stereotype.Component;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ObjectNode;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.UnaryOperator;

/**
 * JSON de eventos com versionamento por header. Consumidores registram "upcasters"
 * (eventType, versãoDeOrigem) que transformam o JSON até a versão que o código atual entende.
 * Versão maior que a conhecida vai para a DLT (não-retryable), nunca é interpretada às cegas.
 */
@Component
public class EventCodec {

    private final JsonMapper mapper;
    private final Map<String, Map<Integer, UnaryOperator<ObjectNode>>> upcasters = new ConcurrentHashMap<>();
    private final Map<String, Integer> currentVersions = new ConcurrentHashMap<>();

    public EventCodec(JsonMapper mapper) {
        this.mapper = mapper;
    }

    public String encode(DomainEvent event) {
        return mapper.writeValueAsString(event);
    }

    public <T extends DomainEvent> T decode(ConsumerRecord<String, String> record, Class<T> type) {
        String eventType = HeaderCodec.require(record.headers(), EventHeaders.EVENT_TYPE);
        Integer received = HeaderCodec.intValue(record.headers(), EventHeaders.SCHEMA_VERSION);
        int version = received == null ? 1 : received;
        int target = currentVersion(eventType);
        if (version > target) {
            throw new UnsupportedSchemaVersionException(eventType, version, target);
        }
        try {
            JsonNode tree = mapper.readTree(record.value());
            if (!(tree instanceof ObjectNode node)) {
                throw new MalformedEventException("Payload de " + eventType + " não é um objeto JSON");
            }
            for (int v = version; v < target; v++) {
                UnaryOperator<ObjectNode> upcaster = upcasters.getOrDefault(eventType, Map.of()).get(v);
                if (upcaster == null) {
                    throw new UnsupportedSchemaVersionException(eventType, version, target);
                }
                node = upcaster.apply(node);
            }
            return mapper.treeToValue(node, type);
        } catch (JacksonException e) {
            throw new MalformedEventException("Payload inválido para " + eventType + ": " + e.getOriginalMessage(), e);
        }
    }

    /** Registra a transformação de v{fromVersion} para v{fromVersion+1} de um tipo de evento. */
    public void registerUpcaster(String eventType, int fromVersion, UnaryOperator<ObjectNode> upcaster) {
        upcasters.computeIfAbsent(eventType, k -> new ConcurrentHashMap<>()).put(fromVersion, upcaster);
        currentVersions.merge(eventType, fromVersion + 1, (current, candidate) -> Math.max(current, candidate));
    }

    public int currentVersion(String eventType) {
        return currentVersions.getOrDefault(eventType, 1);
    }
}
