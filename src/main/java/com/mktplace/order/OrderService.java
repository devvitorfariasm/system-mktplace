package com.mktplace.order;

import com.mktplace.commons.config.MktplaceProperties;
import com.mktplace.commons.events.EventContextHolder;
import com.mktplace.commons.events.OrderCreated;
import com.mktplace.commons.outbox.OutboxPublisher;
import com.mktplace.commons.util.UuidV7;
import com.mktplace.commons.web.ApiException;
import com.mktplace.commons.web.BusinessException;
import com.mktplace.commons.web.NotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.json.JsonMapper;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Clock;
import java.time.OffsetDateTime;
import java.util.HexFormat;
import java.util.List;
import java.util.UUID;

@Service
@Profile("order")
public class OrderService {

    /** Resultado de POST /orders: criado agora ou resposta original repetida (Idempotent-Replayed). */
    public record Creation(OrderDtos.Response response, boolean replayed) {
    }

    private static final Logger log = LoggerFactory.getLogger(OrderService.class);

    private final OrderRepository orders;
    private final CustomerRepository customers;
    private final OrderStatusHistoryRepository history;
    private final IdempotencyKeyStore idempotencyKeys;
    private final OutboxPublisher outbox;
    private final JsonMapper json;
    private final MktplaceProperties props;
    private final Clock clock;

    public OrderService(OrderRepository orders, CustomerRepository customers, OrderStatusHistoryRepository history,
                        IdempotencyKeyStore idempotencyKeys, OutboxPublisher outbox, JsonMapper json,
                        MktplaceProperties props, Clock clock) {
        this.orders = orders;
        this.customers = customers;
        this.history = history;
        this.idempotencyKeys = idempotencyKeys;
        this.outbox = outbox;
        this.json = json;
        this.props = props;
        this.clock = clock;
    }

    /** Pedido + histórico + outbox + chave de idempotência na MESMA transação (ADR 0002). */
    @Transactional
    public Creation create(OrderDtos.CreateRequest request, String idempotencyKey) {
        String requestHash = hash(json.writeValueAsString(request));
        var existing = idempotencyKeys.find(idempotencyKey);
        if (existing.isPresent()) {
            if (!existing.get().requestHash().equals(requestHash)) {
                throw new BusinessException("idempotency-key-reused", "Idempotency-Key reutilizada com corpo diferente",
                        "A chave %s já foi usada para outro pedido (%s)".formatted(idempotencyKey, existing.get().orderId()));
            }
            return new Creation(json.readValue(existing.get().responseBody(), OrderDtos.Response.class), true);
        }
        validate(request);
        var now = OffsetDateTime.now(clock);
        var items = request.items().stream().map(i -> new OrderItem(i.sku(), i.quantity(), i.unitPrice())).toList();
        var order = new Order(UuidV7.generate(), request.customerId(), items, request.currency(), request.shippingAddress(),
                request.notes(), EventContextHolder.get().correlationIdOrNew(), now);
        orders.save(order);
        history.save(new OrderStatusHistory(UuidV7.generate(), order.getId(), null, OrderStatus.CREATED, null, null, true,
                "Pedido criado", now));
        UUID eventId = outbox.enqueue(new OrderCreated(order.getId(), order.getCustomerId(),
                items.stream().map(i -> new OrderCreated.Item(i.sku(), i.quantity(), i.unitPrice())).toList(),
                order.getTotal(), order.getCurrency(), OrderDtos.zoned(now, props)), order.getCorrelationId(), null);
        var response = OrderDtos.Response.from(order, null, props);
        idempotencyKeys.save(idempotencyKey, requestHash, order.getId(), json.writeValueAsString(response));
        log.info("order.created orderId={} customerId={} total={} eventId={}", order.getId(), order.getCustomerId(),
                order.getTotal(), eventId);
        return new Creation(response, false);
    }

    @Transactional(readOnly = true)
    public OrderDtos.Response get(UUID orderId) {
        Order order = find(orderId);
        return OrderDtos.Response.from(order, history.findByOrderIdOrderByOccurredAtAsc(orderId), props);
    }

    Order find(UUID orderId) {
        return orders.findById(orderId).filter(o -> o.getDeletedAt() == null)
                .orElseThrow(() -> new NotFoundException("order-not-found", "Pedido não encontrado: " + orderId));
    }

    private void validate(OrderDtos.CreateRequest request) {
        List<ApiException.FieldError> errors = new java.util.ArrayList<>();
        var seen = new java.util.HashSet<String>();
        for (int i = 0; i < request.items().size(); i++) {
            if (!seen.add(request.items().get(i).sku())) {
                errors.add(new ApiException.FieldError("items[" + i + "].sku", "item duplicado no pedido"));
            }
        }
        if (!customers.existsById(request.customerId())) {
            errors.add(new ApiException.FieldError("customerId", "cliente inexistente"));
        }
        if (!errors.isEmpty()) {
            throw new BusinessException("invalid-order", "Pedido inválido", "O pedido não pode ser criado", errors);
        }
    }

    static String hash(String body) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(body.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }
}
