package dev.saga.shipping;

import dev.saga.messaging.Fault;
import dev.saga.messaging.Message;
import org.springframework.stereotype.Component;
import org.springframework.beans.factory.annotation.Value;

/** A deterministic in-process provider; it deliberately performs no external call. */
@Component
public class SimulatedShippingProvider {
    private final boolean demo;

    public SimulatedShippingProvider(@Value("${demo.enabled:false}") boolean demo) {
        this.demo = demo;
    }

    public boolean book(Message message) {
        return !demo || (message.fault() != Fault.SHIPPING_REJECTED
                && message.fault() != Fault.REFUND_UNAVAILABLE);
    }

    public void cancel(Message message) {
        // The enclosing database transaction is the complete simulated provider operation.
    }
}
