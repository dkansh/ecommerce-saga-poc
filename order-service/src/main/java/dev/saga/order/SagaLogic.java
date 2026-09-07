package dev.saga.order;
import java.util.List;
import dev.saga.messaging.Mode;
import dev.saga.messaging.Type;
public final class SagaLogic {
 private SagaLogic() { }
 public record Decision(String status, int mask, List<Type> commands) { }
 public static Decision advance(String status, int mask, Mode mode, Type event) {
  if (active(status)) {
   if (event==Type.INVENTORY_REJECTED || event==Type.PAYMENT_REJECTED || event==Type.SHIPMENT_REJECTED)
    return cancel(mode);
   if (mode==Mode.CHOREOGRAPHY && event==Type.SHIPMENT_BOOKED)
    return new Decision("COMPLETED",mask,List.of());
   if (mode==Mode.ORCHESTRATION) {
    if (status.equals("RESERVING") && event==Type.INVENTORY_RESERVED)
     return new Decision("PAYING",mask,List.of(Type.CHARGE_PAYMENT));
    if (status.equals("PAYING") && event==Type.PAYMENT_CHARGED)
     return new Decision("SHIPPING",mask,List.of(Type.BOOK_SHIPMENT));
    if (status.equals("SHIPPING") && event==Type.SHIPMENT_BOOKED)
     return new Decision("COMPLETED",mask,List.of());
   }
  }
  if (mode==Mode.ORCHESTRATION) {
   if (status.equals("CANCELLING_SHIPPING") && event==Type.SHIPMENT_CANCELLED)
    return new Decision("REFUNDING",mask|4,List.of(Type.REFUND_PAYMENT));
   if (status.equals("REFUNDING") && event==Type.PAYMENT_REFUNDED)
    return new Decision("RELEASING",mask|2,List.of(Type.RELEASE_INVENTORY));
   if (status.equals("RELEASING") && event==Type.INVENTORY_RELEASED)
    return new Decision("CANCELLED",mask|1,List.of());
  } else if (status.equals("COMPENSATING")) {
   int bit=switch(event) {
    case INVENTORY_RELEASED -> 1;
    case PAYMENT_REFUNDED -> 2;
    case SHIPMENT_CANCELLED -> 4;
    default -> 0;
   };
   int updated=mask|bit;
   return new Decision(updated==7 ? "CANCELLED":"COMPENSATING",updated,List.of());
  }
  return new Decision(status,mask,List.of());
 }
 public static Decision cancel(Mode mode) {
  return mode==Mode.ORCHESTRATION
      ? new Decision("CANCELLING_SHIPPING",0,List.of(Type.CANCEL_SHIPMENT))
      : new Decision("COMPENSATING",0,List.of(Type.CANCEL_SAGA));
 }
 public static boolean active(String state) {
  return List.of("PENDING","RESERVING","PAYING","SHIPPING").contains(state);
 }
 public static boolean compensating(String state) {
  return List.of("COMPENSATING","CANCELLING_SHIPPING","REFUNDING","RELEASING").contains(state);
 }
}
