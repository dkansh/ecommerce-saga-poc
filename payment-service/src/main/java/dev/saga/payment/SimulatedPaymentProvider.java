package dev.saga.payment;

import dev.saga.messaging.Fault;
import dev.saga.messaging.Message;
import org.springframework.stereotype.Component;
import org.springframework.beans.factory.annotation.Value;

/** A deterministic in-process provider; it deliberately performs no external call. */
@Component
public class SimulatedPaymentProvider {
    private final boolean demo;

    public SimulatedPaymentProvider(@Value("${demo.enabled:false}") boolean demo) {
        this.demo = demo;
    }

    public boolean charge(Message message) {
        return !demo || message.fault() != Fault.PAYMENT_DECLINED;
    }

    public void refund(Message message) {
        // The enclosing database transaction is the complete simulated provider operation.
    }
}
