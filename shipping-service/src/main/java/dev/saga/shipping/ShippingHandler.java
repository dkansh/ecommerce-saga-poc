package dev.saga.shipping;

import dev.saga.messaging.Message;
import dev.saga.messaging.MessageHandler;
import dev.saga.messaging.Mode;
import dev.saga.messaging.Outbox;
import dev.saga.messaging.Type;
import java.util.List;
import java.util.Map;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

@Component
public class ShippingHandler implements MessageHandler {
    private final JdbcTemplate jdbc;
    private final Outbox outbox;
    private final SimulatedShippingProvider provider;

    public ShippingHandler(JdbcTemplate jdbc, Outbox outbox, SimulatedShippingProvider provider) {
        this.jdbc = jdbc;
        this.outbox = outbox;
        this.provider = provider;
    }

    @Override
    public boolean accepts(Message message) {
        return switch (message.type()) {
            case BOOK_SHIPMENT, CANCEL_SHIPMENT -> message.mode() == Mode.ORCHESTRATION;
            case PAYMENT_CHARGED, CANCEL_SAGA -> message.mode() == Mode.CHOREOGRAPHY;
            default -> false;
        };
    }

    @Override
    @Transactional(propagation = Propagation.MANDATORY)
    public void handle(Message message) {
        switch (message.type()) {
            case BOOK_SHIPMENT, PAYMENT_CHARGED -> book(message);
            case CANCEL_SHIPMENT, CANCEL_SAGA -> cancel(message);
            default -> throw new IllegalArgumentException("Unsupported shipping message: " + message.type());
        }
    }

    private void book(Message message) {
        requireShippingPayload(message);
        var existing = shipment(message);
        if (!existing.isEmpty()) {
            assertSamePayload(existing.getFirst(), message);
            var state = (String) existing.getFirst().get("state");
            outbox.add(message.next("BOOKED".equals(state) ? Type.SHIPMENT_BOOKED : Type.SHIPMENT_REJECTED));
            return;
        }

        boolean booked = provider.book(message);
        jdbc.update("insert into shipment(saga_id,sku,quantity,state) values (?,?,?,?)",
                message.sagaId(), message.sku(), message.quantity(), booked ? "BOOKED" : "REJECTED");
        outbox.add(message.next(booked ? Type.SHIPMENT_BOOKED : Type.SHIPMENT_REJECTED));
    }

    private void cancel(Message message) {
        requireShippingPayload(message);
        var existing = shipment(message);
        if (existing.isEmpty()) {
            jdbc.update("insert into shipment(saga_id,sku,quantity,state) values (?,?,?,'CANCELLED')",
                    message.sagaId(), message.sku(), message.quantity());
        } else {
            var row = existing.getFirst();
            assertSamePayload(row, message);
            if ("BOOKED".equals(row.get("state"))) {
                provider.cancel(message);
            }
            jdbc.update("""
                    update shipment set state = 'CANCELLED', updated_at = current_timestamp
                     where saga_id = ? and state <> 'CANCELLED'
                    """, message.sagaId());
        }
        outbox.add(message.next(Type.SHIPMENT_CANCELLED));
    }

    private List<Map<String, Object>> shipment(Message message) {
        return jdbc.queryForList("select sku, quantity, state from shipment where saga_id = ? for update", message.sagaId());
    }

    private static void requireShippingPayload(Message message) {
        if (message.sku() == null || message.sku().isBlank() || message.quantity() <= 0) {
            throw new IllegalArgumentException("Shipping command requires sku and positive quantity");
        }
    }

    private static void assertSamePayload(Map<String, Object> row, Message message) {
        if (!message.sku().equals(row.get("sku"))
                || message.quantity() != ((Number) row.get("quantity")).intValue()) {
            throw new IllegalArgumentException("Conflicting shipping payload for saga " + message.sagaId());
        }
    }
}
