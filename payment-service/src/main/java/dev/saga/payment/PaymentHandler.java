package dev.saga.payment;

import dev.saga.messaging.Fault;
import dev.saga.messaging.Faults;
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
public class PaymentHandler implements MessageHandler {
    private final JdbcTemplate jdbc;
    private final Outbox outbox;
    private final Faults faults;
    private final SimulatedPaymentProvider provider;

    public PaymentHandler(JdbcTemplate jdbc, Outbox outbox, Faults faults, SimulatedPaymentProvider provider) {
        this.jdbc = jdbc;
        this.outbox = outbox;
        this.faults = faults;
        this.provider = provider;
    }

    @Override
    public boolean accepts(Message message) {
        return switch (message.type()) {
            case CHARGE_PAYMENT, REFUND_PAYMENT -> message.mode() == Mode.ORCHESTRATION;
            case INVENTORY_RESERVED, CANCEL_SAGA -> message.mode() == Mode.CHOREOGRAPHY;
            default -> false;
        };
    }

    @Override
    @Transactional(propagation = Propagation.MANDATORY)
    public void handle(Message message) {
        switch (message.type()) {
            case CHARGE_PAYMENT, INVENTORY_RESERVED -> charge(message);
            case REFUND_PAYMENT, CANCEL_SAGA -> refund(message);
            default -> throw new IllegalArgumentException("Unsupported payment message: " + message.type());
        }
    }

    private void charge(Message message) {
        requireAmount(message);
        var existing = payment(message);
        if (!existing.isEmpty()) {
            assertSamePayload(existing.getFirst(), message);
            var state = (String) existing.getFirst().get("state");
            outbox.add(message.next("CHARGED".equals(state) ? Type.PAYMENT_CHARGED : Type.PAYMENT_REJECTED));
            return;
        }

        faults.check(message, Fault.PAYMENT_TRANSIENT, true);
        faults.check(message, Fault.PAYMENT_UNAVAILABLE, false);
        boolean charged = provider.charge(message);
        jdbc.update("insert into payment(saga_id,amount_cents,state) values (?,?,?)",
                message.sagaId(), message.amountCents(), charged ? "CHARGED" : "REJECTED");
        outbox.add(message.next(charged ? Type.PAYMENT_CHARGED : Type.PAYMENT_REJECTED));
    }

    private void refund(Message message) {
        requireAmount(message);
        var existing = payment(message);
        if (existing.isEmpty()) {
            jdbc.update("insert into payment(saga_id,amount_cents,state) values (?,?,'REFUNDED')",
                    message.sagaId(), message.amountCents());
        } else {
            var row = existing.getFirst();
            assertSamePayload(row, message);
            if ("CHARGED".equals(row.get("state"))) {
                faults.check(message, Fault.REFUND_UNAVAILABLE, false);
                provider.refund(message);
            }
            jdbc.update("""
                    update payment set state = 'REFUNDED', updated_at = current_timestamp
                     where saga_id = ? and state <> 'REFUNDED'
                    """, message.sagaId());
        }
        outbox.add(message.next(Type.PAYMENT_REFUNDED));
    }

    private List<Map<String, Object>> payment(Message message) {
        return jdbc.queryForList("select amount_cents, state from payment where saga_id = ? for update", message.sagaId());
    }

    private static void requireAmount(Message message) {
        if (message.amountCents() < 0) {
            throw new IllegalArgumentException("Payment amount cannot be negative");
        }
    }

    private static void assertSamePayload(Map<String, Object> row, Message message) {
        if (message.amountCents() != ((Number) row.get("amount_cents")).longValue()) {
            throw new IllegalArgumentException("Conflicting payment payload for saga " + message.sagaId());
        }
    }
}
