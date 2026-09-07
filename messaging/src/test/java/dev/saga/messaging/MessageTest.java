package dev.saga.messaging;
import org.junit.jupiter.api.Test;
import java.util.UUID;
import static org.assertj.core.api.Assertions.*;
class MessageTest {
 @Test void rejectsUnknownVersionBeforeBusinessProcessing() {
  var m = new Message(2, UUID.randomUUID(), UUID.randomUUID(), Mode.ORCHESTRATION, Type.ORDER_CREATED, "SKU-1", 1, 100, Fault.NONE);
  assertThatThrownBy(m::validate).isInstanceOf(IllegalArgumentException.class);
 }
 @Test void rejectsInvalidQuantityAndMissingCorrelation() {
  var m = new Message(1, UUID.randomUUID(), null, Mode.ORCHESTRATION, Type.ORDER_CREATED, "SKU-1", 0, 100, Fault.NONE);
  assertThatThrownBy(m::validate).isInstanceOf(IllegalArgumentException.class);
 }
}
