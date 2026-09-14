package com.mktplace.order;

import com.mktplace.commons.config.MktplaceProperties;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import org.jspecify.annotations.Nullable;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/** Contratos REST de pedidos (seção 6.2). Dinheiro em centavos, datas com offset. */
public final class OrderDtos {

    private OrderDtos() {
    }

    public record ItemRequest(
            @NotBlank(message = "sku é obrigatório") String sku,
            @Min(value = 1, message = "quantidade deve ser maior que zero") int quantity,
            @Min(value = 0, message = "preço não pode ser negativo") long unitPrice) {
    }

    public record CreateRequest(
            @NotNull(message = "customerId é obrigatório") UUID customerId,
            @NotEmpty(message = "informe ao menos um item") List<@Valid ItemRequest> items,
            @NotBlank(message = "currency é obrigatória") @Pattern(regexp = "[A-Z]{3}", message = "currency deve ter 3 letras (ex.: BRL)") String currency,
            @Nullable Map<String, String> shippingAddress,
            @Nullable String notes) {
    }

    public record ItemResponse(String sku, int quantity, long unitPrice, long subtotal) {
    }

    public record StatusHistoryResponse(@Nullable OrderStatus from, OrderStatus to, @Nullable String eventId,
                                        @Nullable String eventType, boolean applied, @Nullable String reason,
                                        OffsetDateTime occurredAt) {
    }

    public record Response(
            UUID id,
            UUID customerId,
            OrderStatus status,
            boolean finished,
            long total,
            String currency,
            List<ItemResponse> items,
            @Nullable Map<String, String> shippingAddress,
            @Nullable String notes,
            long version,
            String correlationId,
            OffsetDateTime createdAt,
            OffsetDateTime updatedAt,
            @Nullable List<StatusHistoryResponse> statusHistory,
            Map<String, String> _links) {

        public static Response from(Order o, @Nullable List<OrderStatusHistory> history, MktplaceProperties props) {
            String base = "/api/v1/orders/" + o.getId();
            return new Response(o.getId(), o.getCustomerId(), o.getStatus(), o.getStatus().isTerminal(), o.getTotal(),
                    o.getCurrency(),
                    o.getItems().stream().map(i -> new ItemResponse(i.sku(), i.quantity(), i.unitPrice(), i.subtotal())).toList(),
                    o.getShippingAddress(), o.getNotes(), o.getVersion(), o.getCorrelationId(),
                    zoned(o.getCreatedAt(), props), zoned(o.getUpdatedAt(), props),
                    history == null ? null : history.stream().map(h -> new StatusHistoryResponse(h.getFromStatus(),
                            h.getToStatus(), h.getEventId(), h.getEventType(), h.isApplied(), h.getReason(),
                            zoned(h.getOccurredAt(), props))).toList(),
                    Map.of("self", base, "timeline", base + "/timeline", "stream", base + "/timeline/stream"));
        }
    }

    public record CustomerRequest(
            @NotBlank(message = "Nome é obrigatório") String name,
            @NotBlank(message = "Email é obrigatório") @jakarta.validation.constraints.Email(message = "Email inválido") String email) {
    }

    public record CustomerResponse(UUID id, String name, String email, long version, OffsetDateTime createdAt) {

        public static CustomerResponse from(Customer c, MktplaceProperties props) {
            return new CustomerResponse(c.getId(), c.getName(), c.getEmail(), c.getVersion(), zoned(c.getCreatedAt(), props));
        }
    }

    static OffsetDateTime zoned(OffsetDateTime at, MktplaceProperties props) {
        return at.atZoneSameInstant(props.zone()).toOffsetDateTime();
    }
}
