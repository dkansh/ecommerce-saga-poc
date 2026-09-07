package dev.saga.inventory;

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
public class InventoryHandler implements MessageHandler {
    private final JdbcTemplate jdbc;
    private final Outbox outbox;

    public InventoryHandler(JdbcTemplate jdbc, Outbox outbox) {
        this.jdbc = jdbc;
        this.outbox = outbox;
    }

    @Override
    public boolean accepts(Message message) {
        return switch (message.type()) {
            case RESERVE_INVENTORY, RELEASE_INVENTORY -> message.mode() == Mode.ORCHESTRATION;
            case ORDER_CREATED, CANCEL_SAGA -> message.mode() == Mode.CHOREOGRAPHY;
            default -> false;
        };
    }

    @Override
    @Transactional(propagation = Propagation.MANDATORY)
    public void handle(Message message) {
        switch (message.type()) {
            case RESERVE_INVENTORY, ORDER_CREATED -> reserve(message);
            case RELEASE_INVENTORY, CANCEL_SAGA -> release(message);
            default -> throw new IllegalArgumentException("Unsupported inventory message: " + message.type());
        }
    }

    private void reserve(Message message) {
        requireInventoryPayload(message);
        var existing = reservation(message);
        if (!existing.isEmpty()) {
            assertSamePayload(existing.getFirst(), message);
            var state = (String) existing.getFirst().get("state");
            outbox.add(message.next("RESERVED".equals(state) ? Type.INVENTORY_RESERVED : Type.INVENTORY_REJECTED));
            return;
        }

        int changed = jdbc.update("""
                update inventory_stock
                   set available = available - ?
                 where sku = ? and available >= ?
                """, message.quantity(), message.sku(), message.quantity());
        String state = changed == 1 ? "RESERVED" : "REJECTED";
        jdbc.update("insert into inventory_reservation(saga_id,sku,quantity,state) values (?,?,?,?)",
                message.sagaId(), message.sku(), message.quantity(), state);
        outbox.add(message.next(changed == 1 ? Type.INVENTORY_RESERVED : Type.INVENTORY_REJECTED));
    }

    private void release(Message message) {
        requireInventoryPayload(message);
        var existing = reservation(message);
        if (existing.isEmpty()) {
            jdbc.update("insert into inventory_reservation(saga_id,sku,quantity,state) values (?,?,?,'RELEASED')",
                    message.sagaId(), message.sku(), message.quantity());
        } else {
            var row = existing.getFirst();
            assertSamePayload(row, message);
            if ("RESERVED".equals(row.get("state"))) {
                jdbc.update("update inventory_stock set available = available + ? where sku = ?",
                        message.quantity(), message.sku());
            }
            jdbc.update("""
                    update inventory_reservation
                       set state = 'RELEASED', updated_at = current_timestamp
                     where saga_id = ? and state <> 'RELEASED'
                    """, message.sagaId());
        }
        outbox.add(message.next(Type.INVENTORY_RELEASED));
    }

    private List<Map<String, Object>> reservation(Message message) {
        return jdbc.queryForList("select sku, quantity, state from inventory_reservation where saga_id = ? for update",
                message.sagaId());
    }

    private static void requireInventoryPayload(Message message) {
        if (message.sku() == null || message.sku().isBlank() || message.quantity() <= 0) {
            throw new IllegalArgumentException("Inventory command requires sku and positive quantity");
        }
    }

    private static void assertSamePayload(Map<String, Object> row, Message message) {
        if (!message.sku().equals(row.get("sku"))
                || message.quantity() != ((Number) row.get("quantity")).intValue()) {
            throw new IllegalArgumentException("Conflicting inventory payload for saga " + message.sagaId());
        }
    }
}
