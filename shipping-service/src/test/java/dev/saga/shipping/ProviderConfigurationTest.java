package dev.saga.shipping;
import dev.saga.messaging.*;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import static org.assertj.core.api.Assertions.*;
class ProviderConfigurationTest {
 @Test void demoDisabledIgnoresPendingShippingFault() {
  try (var context=new AnnotationConfigApplicationContext(SimulatedShippingProvider.class)) {
   var m=new Message(1,UUID.randomUUID(),UUID.randomUUID(),Mode.ORCHESTRATION,Type.BOOK_SHIPMENT,"SKU-1",1,100,Fault.SHIPPING_REJECTED);
   assertThat(context.getBean(SimulatedShippingProvider.class).book(m)).isTrue();
  }
 }
}
