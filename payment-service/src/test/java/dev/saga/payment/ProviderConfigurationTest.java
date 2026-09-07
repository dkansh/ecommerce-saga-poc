package dev.saga.payment;
import dev.saga.messaging.*;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import static org.assertj.core.api.Assertions.*;
class ProviderConfigurationTest {
 @Test void demoDisabledIgnoresPendingDeclineFault() {
  try (var context=new AnnotationConfigApplicationContext(SimulatedPaymentProvider.class)) {
   var m=new Message(1,UUID.randomUUID(),UUID.randomUUID(),Mode.ORCHESTRATION,Type.CHARGE_PAYMENT,"SKU-1",1,100,Fault.PAYMENT_DECLINED);
   assertThat(context.getBean(SimulatedPaymentProvider.class).charge(m)).isTrue();
  }
 }
}
