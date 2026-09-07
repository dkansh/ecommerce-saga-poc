package dev.saga.messaging;
import java.util.UUID;
public record Message(int version, UUID id, UUID sagaId, Mode mode, Type type,
                      String sku, int quantity, long amountCents, Fault fault) {
 public Message next(Type nextType) {
  return new Message(version, UUID.randomUUID(), sagaId, mode, nextType, sku, quantity, amountCents, fault);
 }
 public void validate() {
  if (version!=1) throw new IllegalArgumentException("Unsupported message version: "+version);
  if (id==null || sagaId==null || mode==null || type==null || fault==null)
   throw new IllegalArgumentException("Message identity, mode, type and fault are required");
  if (sku==null || !sku.matches("[A-Z0-9-]{1,40}") || quantity<1 || quantity>1000 || amountCents<1 || amountCents>1_000_000_000L)
   throw new IllegalArgumentException("Invalid SKU, quantity or amountCents");
 }
}
