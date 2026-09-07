package dev.saga.order;
import org.junit.jupiter.api.Test;
import dev.saga.messaging.Mode;
import dev.saga.messaging.Type;
import static org.assertj.core.api.Assertions.*;
class SagaLogicTest {
 @Test void orchestratorChargesOnlyAfterReservation() {
  var d=SagaLogic.advance("RESERVING",0,Mode.ORCHESTRATION,Type.INVENTORY_RESERVED);
  assertThat(d.status()).isEqualTo("PAYING");
  assertThat(d.commands()).containsExactly(Type.CHARGE_PAYMENT);
 }
 @Test void delayedSuccessCannotCompleteCompensatingOrder() {
  var d=SagaLogic.advance("REFUNDING",0,Mode.ORCHESTRATION,Type.SHIPMENT_BOOKED);
  assertThat(d.status()).isEqualTo("REFUNDING");
  assertThat(d.commands()).isEmpty();
 }
 @Test void compensationRunsShippingThenPaymentThenInventory() {
  var d=SagaLogic.cancel(Mode.ORCHESTRATION);
  assertThat(d.commands()).containsExactly(Type.CANCEL_SHIPMENT);
  d=SagaLogic.advance(d.status(),d.mask(),Mode.ORCHESTRATION,Type.SHIPMENT_CANCELLED);
  assertThat(d.commands()).containsExactly(Type.REFUND_PAYMENT);
  d=SagaLogic.advance(d.status(),d.mask(),Mode.ORCHESTRATION,Type.PAYMENT_REFUNDED);
  assertThat(d.commands()).containsExactly(Type.RELEASE_INVENTORY);
  d=SagaLogic.advance(d.status(),d.mask(),Mode.ORCHESTRATION,Type.INVENTORY_RELEASED);
  assertThat(d.status()).isEqualTo("CANCELLED");
 }
 @Test void choreographyNeedsThreeDistinctCompensationAcknowledgements() {
  var d=SagaLogic.cancel(Mode.CHOREOGRAPHY);
  assertThat(d.commands()).containsExactly(Type.CANCEL_SAGA);
  for(int i=0;i<3;i++) d=SagaLogic.advance(d.status(),d.mask(),Mode.CHOREOGRAPHY,Type.PAYMENT_REFUNDED);
  assertThat(d.status()).isEqualTo("COMPENSATING");
  d=SagaLogic.advance(d.status(),d.mask(),Mode.CHOREOGRAPHY,Type.INVENTORY_RELEASED);
  assertThat(d.status()).isEqualTo("COMPENSATING");
  d=SagaLogic.advance(d.status(),d.mask(),Mode.CHOREOGRAPHY,Type.SHIPMENT_CANCELLED);
  assertThat(d.status()).isEqualTo("CANCELLED");
 }
 @Test void terminalStatesIgnoreLateEvents() {
  assertThat(SagaLogic.advance("COMPLETED",0,Mode.ORCHESTRATION,Type.PAYMENT_REJECTED).status()).isEqualTo("COMPLETED");
  assertThat(SagaLogic.advance("CANCELLED",7,Mode.CHOREOGRAPHY,Type.SHIPMENT_BOOKED).status()).isEqualTo("CANCELLED");
 }
 @Test void choreographyDoesNotIssueForwardCommands() {
  var d=SagaLogic.advance("PENDING",0,Mode.CHOREOGRAPHY,Type.INVENTORY_RESERVED);
  assertThat(d.commands()).isEmpty();
  assertThat(SagaLogic.advance("PENDING",0,Mode.CHOREOGRAPHY,Type.SHIPMENT_BOOKED).status()).isEqualTo("COMPLETED");
 }
}
