package com.mktplace.inventory;

import com.mktplace.commons.events.OrderCreated;
import com.mktplace.commons.events.StockReserved;
import com.mktplace.commons.events.StockUnavailable;
import com.mktplace.commons.util.UuidV7;
import org.springframework.context.annotation.Profile;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.Clock;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Reserva atômica: "UPDATE ... WHERE total - reserved >= :qty RETURNING" por item, tudo numa transação.
 * Se um item falha, a transação inteira volta (REQUIRES_NEW) e nada fica reservado. Dois consumers
 * concorrentes no mesmo SKU serializam no lock de linha do UPDATE: só um leva a última unidade.
 */
@Service
@Profile("inventory")
public class ReservationService {

    public sealed interface Outcome permits Reserved, Unavailable {
    }

    public record Reserved(List<StockReserved.Reservation> reservations) implements Outcome {
    }

    public record Unavailable(List<StockUnavailable.Shortage> shortages) implements Outcome {
    }

    /** Exceção interna para forçar rollback do REQUIRES_NEW; nunca sai do serviço. */
    static final class InsufficientStock extends RuntimeException {
        InsufficientStock(String sku) {
            super("estoque insuficiente para " + sku);
        }
    }

    private final JdbcClient jdbc;
    private final Clock clock;
    private final ReservationService self;

    public ReservationService(JdbcClient jdbc, Clock clock, @org.springframework.context.annotation.Lazy ReservationService self) {
        this.jdbc = jdbc;
        this.clock = clock;
        this.self = self;   // chamada via proxy para o REQUIRES_NEW valer
    }

    public Outcome reserve(UUID orderId, List<OrderCreated.Item> items, String causedByEventId) {
        try {
            return new Reserved(self.reserveAllOrFail(orderId, items, causedByEventId));
        } catch (InsufficientStock e) {
            return new Unavailable(shortages(items));   // lido depois do rollback: reflete o estoque real
        }
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public List<StockReserved.Reservation> reserveAllOrFail(UUID orderId, List<OrderCreated.Item> items, String causedByEventId) {
        List<StockReserved.Reservation> reservations = new ArrayList<>();
        for (OrderCreated.Item item : items) {
            var updated = jdbc.sql("""
                    UPDATE products SET reserved = reserved + :qty, version = version + 1, updated_at = now()
                    WHERE sku = :sku AND active AND total - reserved >= :qty
                    RETURNING total, reserved
                    """)
                    .param("qty", item.quantity()).param("sku", item.sku())
                    .query((rs, i) -> new int[]{rs.getInt("total"), rs.getInt("reserved")})
                    .optional();
            if (updated.isEmpty()) {
                throw new InsufficientStock(item.sku());   // rollback de tudo que já foi reservado
            }
            UUID reservationId = UuidV7.generate();
            jdbc.sql("INSERT INTO reservations (id, order_id, sku, quantity, status, event_id) VALUES (:id, :orderId, :sku, :qty, 'ACTIVE', :eventId)")
                    .param("id", reservationId).param("orderId", orderId).param("sku", item.sku())
                    .param("qty", item.quantity()).param("eventId", causedByEventId).update();
            jdbc.sql("""
                    INSERT INTO stock_movements (id, sku, order_id, delta, kind, reason, caused_by_event_id, balance_total, balance_reserved)
                    VALUES (:id, :sku, :orderId, :delta, 'RESERVE', :reason, :eventId, :total, :reserved)
                    """)
                    .param("id", UuidV7.generate()).param("sku", item.sku()).param("orderId", orderId)
                    .param("delta", -item.quantity()).param("reason", "Reserva do pedido " + orderId)
                    .param("eventId", causedByEventId).param("total", updated.get()[0]).param("reserved", updated.get()[1])
                    .update();
            reservations.add(new StockReserved.Reservation(reservationId, item.sku(), item.quantity()));
        }
        return reservations;
    }

    /** Disponível atual de cada item, para explicar o que faltou. */
    private List<StockUnavailable.Shortage> shortages(List<OrderCreated.Item> items) {
        List<StockUnavailable.Shortage> shortages = new ArrayList<>();
        for (OrderCreated.Item item : items) {
            int available = jdbc.sql("SELECT total - reserved FROM products WHERE sku = :sku AND active")
                    .param("sku", item.sku()).query(Integer.class).optional().orElse(0);
            if (available < item.quantity()) {
                shortages.add(new StockUnavailable.Shortage(item.sku(), item.quantity(), Math.max(available, 0)));
            }
        }
        return shortages;
    }

    OffsetDateTime now() {
        return OffsetDateTime.now(clock);
    }
}
