package com.mktplace.flow;

import com.mktplace.commons.it.AbstractKafkaIT;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.web.client.RestClient;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.time.Duration;
import java.util.Map;
import java.util.UUID;

/**
 * Fluxo ponta a ponta: os cinco serviços ativos no MESMO processo (profiles), um banco com todas as
 * migrations, Kafka e Postgres reais. Reusa os containers já iniciados pelos ITs do commons.
 */
// Propriedades aqui têm precedência sobre os application-<profile>.yml, que se sobrescrevem entre si
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "mktplace.service-name=test-service",
        "mktplace.audit-url=http://localhost:${local.server.port}",
        "mktplace.outbox.relay-delay-ms=100",
        "mktplace.payment.approval-rate=1.0",
        "mktplace.payment.latency-min-ms=0",
        "mktplace.payment.latency-max-ms=0",
        "spring.flyway.locations=classpath:db/commons,classpath:db/migration/order,classpath:db/migration/payment,"
                + "classpath:db/migration/inventory,classpath:db/migration/notification,classpath:db/migration/audit"})
@ActiveProfiles({"test", "order", "payment", "inventory", "notification", "audit"})
public abstract class AbstractFlowIT {

    @ServiceConnection
    static final KafkaContainer KAFKA = AbstractKafkaIT.kafka();
    @ServiceConnection
    static final PostgreSQLContainer POSTGRES = AbstractKafkaIT.postgres();

    protected static final Duration TIMEOUT = Duration.ofSeconds(60);
    protected static final JsonMapper JSON = JsonMapper.builder().build();

    public record Reply(int status, JsonNode body, Map<String, String> headers) {
        public String text(String field) {
            return body.get(field).asString();
        }
    }

    @Value("${local.server.port}")
    int port;

    protected Reply call(HttpMethod method, String path, Object body, Map<String, String> headers) {
        var request = RestClient.create("http://localhost:" + port).method(method).uri(path)
                .contentType(MediaType.APPLICATION_JSON);
        headers.forEach(request::header);
        if (body != null) {
            request = request.body(JSON.writeValueAsString(body));
        }
        return request.exchange((req, res) -> {
            String text = res.bodyTo(String.class);
            JsonNode node = text == null || text.isBlank() ? JSON.nullNode() : JSON.readTree(text);
            Map<String, String> h = new java.util.HashMap<>();
            res.getHeaders().forEach((k, v) -> h.put(k.toLowerCase(), v.isEmpty() ? "" : v.getFirst()));   // chaves em minúsculas
            return new Reply(res.getStatusCode().value(), node, h);
        });
    }

    protected Reply get(String path) {
        return call(HttpMethod.GET, path, null, Map.of());
    }

    protected Reply post(String path, Object body) {
        return call(HttpMethod.POST, path, body, Map.of());
    }

    protected UUID newCustomer() {
        String suffix = UUID.randomUUID().toString().substring(0, 8);
        Reply r = post("/api/v1/customers", Map.of("name", "Cliente " + suffix, "email", suffix + "@teste.dev"));
        if (r.status() != 201) {
            throw new AssertionError("cliente não criado: " + r.status() + " " + r.body());
        }
        return UUID.fromString(r.text("id"));
    }

    protected String newProduct(int stock) {
        String sku = "SKU-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase();
        Reply r = post("/api/v1/products", Map.of("sku", sku, "name", "Produto " + sku, "price", 4990, "initialStock", stock));
        if (r.status() != 201) {
            throw new AssertionError("produto não criado: " + r.status() + " " + r.body());
        }
        return sku;
    }

    protected Reply createOrder(UUID customerId, String sku, int quantity, String idempotencyKey) {
        var body = Map.of("customerId", customerId, "currency", "BRL",
                "items", java.util.List.of(Map.of("sku", sku, "quantity", quantity, "unitPrice", 4990)),
                "shippingAddress", Map.of("street", "Rua A", "city", "Curitiba", "zip", "80000-000"),
                "notes", "Entregar na portaria");
        return call(HttpMethod.POST, "/api/v1/orders", body, Map.of("Idempotency-Key", idempotencyKey));
    }
}
