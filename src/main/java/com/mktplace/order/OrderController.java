package com.mktplace.order;

import com.mktplace.commons.web.BadRequestException;
import jakarta.validation.Valid;
import org.springframework.context.annotation.Profile;
import org.springframework.core.env.Environment;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestClient;

import java.net.URI;
import java.util.UUID;

/** Rotas de pedidos da fatia vertical: criar, detalhar e a linha do tempo (proxy para o audit-service). */
@RestController
@RequestMapping("/api/v1/orders")
@Profile("order")
public class OrderController {

    public static final String IDEMPOTENCY_KEY = "Idempotency-Key";
    public static final String IDEMPOTENT_REPLAYED = "Idempotent-Replayed";

    private final OrderService service;
    private final Environment env;

    public OrderController(OrderService service, Environment env) {
        this.service = service;
        this.env = env;
    }

    @PostMapping
    public ResponseEntity<OrderDtos.Response> create(@Valid @RequestBody OrderDtos.CreateRequest request,
                                                     @RequestHeader(name = IDEMPOTENCY_KEY, required = false) String idempotencyKey) {
        if (idempotencyKey == null || idempotencyKey.isBlank()) {
            throw new BadRequestException("idempotency-key-required", "Header Idempotency-Key é obrigatório em POST /orders");
        }
        OrderService.Creation created = service.create(request, idempotencyKey.trim());
        var response = created.response();
        if (created.replayed()) {
            return ResponseEntity.ok().header(IDEMPOTENT_REPLAYED, "true").eTag("\"" + response.version() + "\"").body(response);
        }
        return ResponseEntity.created(URI.create(response._links().get("self")))
                .eTag("\"" + response.version() + "\"").body(response);
    }

    @GetMapping("/{orderId}")
    public ResponseEntity<OrderDtos.Response> get(@PathVariable UUID orderId) {
        var response = service.get(orderId);
        return ResponseEntity.ok().eTag("\"" + response.version() + "\"").body(response);
    }

    /** Rota central do desafio: o order-service valida que o pedido existe e delega ao audit-service. */
    @GetMapping(value = "/{orderId}/timeline", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<String> timeline(@PathVariable UUID orderId) {
        service.find(orderId);
        // base URL resolvida por chamada: nos testes o audit-service responde na mesma porta aleatória
        String body = RestClient.create(env.getRequiredProperty("mktplace.audit-url"))
                .get().uri("/api/v1/audit/orders/{id}/timeline", orderId).retrieve().body(String.class);
        return ResponseEntity.status(HttpStatus.OK).contentType(MediaType.APPLICATION_JSON).body(body);
    }
}
