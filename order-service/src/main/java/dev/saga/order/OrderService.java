package dev.saga.order;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.*;
import dev.saga.messaging.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.server.ResponseStatusException;
import tools.jackson.databind.ObjectMapper;

@Service
public class OrderService implements MessageHandler {
 private final JdbcTemplate jdbc;
 private final Outbox outbox;
 private final ObjectMapper json;
 private final int timeout;
 private final int compensationTimeout;
 private final boolean demo;
 public OrderService(JdbcTemplate jdbc, Outbox outbox, ObjectMapper json,
    @Value("${saga.timeout-seconds:30}") int timeout,
    @Value("${saga.compensation-timeout-seconds:30}") int compensationTimeout,
    @Value("${demo.enabled:false}") boolean demo) {
  this.jdbc=jdbc; this.outbox=outbox; this.json=json;
  this.timeout=timeout; this.compensationTimeout=compensationTimeout; this.demo=demo;
 }
 @Transactional
 public Map<String,Object> create(String key, CreateOrder request) {
  if (key==null || !key.matches("[A-Za-z0-9._:-]{1,128}"))
   throw new IllegalArgumentException("Idempotency-Key must contain 1-128 letters, digits, dot, underscore, colon or hyphen");
  if (!demo && request.fault()!=Fault.NONE)
   throw new IllegalArgumentException("Fault injection requires DEMO_ENABLED=true");
  UUID id=UUID.randomUUID();
  Message initial=new Message(1,UUID.randomUUID(),id,request.mode(),
      request.mode()==Mode.ORCHESTRATION ? Type.RESERVE_INVENTORY : Type.ORDER_CREATED,
      request.sku(),request.quantity(),request.amountCents(),request.fault());
  initial.validate();
  String status=request.mode()==Mode.ORCHESTRATION ? "RESERVING" : "PENDING";
  int inserted=jdbc.update("""
     insert into orders(id,idempotency_key,request_json,message_json,status,deadline)
     values (?,?,?,?,?,?) on conflict(idempotency_key) do nothing
     """,id,key,json.writeValueAsString(request),json.writeValueAsString(initial),status,
     Timestamp.from(Instant.now().plusSeconds(timeout)));
  if (inserted==0) {
   var existing=jdbc.queryForMap("select id,request_json from orders where idempotency_key=?",key);
   if (!request.equals(json.readValue((String)existing.get("request_json"),CreateOrder.class)))
    throw new ResponseStatusException(HttpStatus.CONFLICT,"Idempotency-Key already used for different request");
   return get((UUID)existing.get("id"));
  }
  outbox.add(initial);
  history(id,"ORDER_ACCEPTED",status);
  return get(id);
 }
 public Map<String,Object> get(UUID id) {
  var row=find(id,false);
  var message=message(row);
  var result=new LinkedHashMap<String,Object>();
  result.put("id",id); result.put("status",row.get("status")); result.put("mode",message.mode());
  result.put("sku",message.sku()); result.put("quantity",message.quantity());
  result.put("amountCents",message.amountCents()); result.put("fault",message.fault());
  result.put("reason",row.get("reason")); result.put("cancelMask",row.get("cancel_mask"));
  result.put("deadline",row.get("deadline")); result.put("createdAt",row.get("created_at"));
  return result;
 }
 public List<Map<String,Object>> history(UUID id) {
  find(id,false);
  return jdbc.queryForList("select event,status,created_at from saga_history where saga_id=? order by sequence",id);
 }
 @Override public boolean accepts(Message m) {
  return switch(m.type()) {
   case INVENTORY_RESERVED, INVENTORY_REJECTED, PAYMENT_CHARGED, PAYMENT_REJECTED,
        SHIPMENT_BOOKED, SHIPMENT_REJECTED, SHIPMENT_CANCELLED, PAYMENT_REFUNDED,
        INVENTORY_RELEASED -> true;
   default -> false;
  };
 }
 @Override public void handle(Message incoming) {
  var rows=jdbc.queryForList("select * from orders where id=? for update",incoming.sagaId());
  if (rows.isEmpty()) return; // Events for another deployment or a participant-only recovery exercise.
  var row=rows.getFirst();
  var original=message(row);
  if (incoming.mode()!=original.mode() || !incoming.sku().equals(original.sku()) ||
      incoming.quantity()!=original.quantity() || incoming.amountCents()!=original.amountCents() ||
      incoming.fault()!=original.fault())
   throw new IllegalArgumentException("Event payload differs from accepted order");
  String old=(String)row.get("status");
  var decision=SagaLogic.advance(old,((Number)row.get("cancel_mask")).intValue(),original.mode(),incoming.type());
  if (decision.status().equals(old) && decision.mask()==((Number)row.get("cancel_mask")).intValue()) return;
  boolean startingCompensation=SagaLogic.active(old) && SagaLogic.compensating(decision.status());
  apply(original,decision,startingCompensation ? incoming.type().name() : (String)row.get("reason"),startingCompensation);
  history(incoming.sagaId(),incoming.type().name(),decision.status());
 }
 @Transactional
 public Map<String,Object> cancel(UUID id) {
  var row=find(id,true);
  String status=(String)row.get("status");
  if ("COMPLETED".equals(status)) throw new ResponseStatusException(HttpStatus.CONFLICT,"Completed checkout cannot be cancelled by this POC");
  if (SagaLogic.active(status)) {
   var m=message(row);
   apply(m,SagaLogic.cancel(m.mode()),"CLIENT_CANCELLED",true);
   history(id,"CLIENT_CANCELLED",SagaLogic.cancel(m.mode()).status());
  }
  return get(id);
 }
 @Transactional
 public Map<String,Object> retry(UUID id) {
  var row=find(id,true);
  String status=(String)row.get("status");
  if (!"MANUAL_INTERVENTION".equals(status) && !SagaLogic.compensating(status))
   throw new ResponseStatusException(HttpStatus.CONFLICT,"Only incomplete compensation can be retried");
  var m=message(row);
  var decision=SagaLogic.cancel(m.mode());
  apply(m,decision,(String)row.get("reason"),true);
  history(id,"COMPENSATION_RETRIED",decision.status());
  return get(id);
 }
 @Transactional
 public void expire(UUID id) {
  var row=find(id,true);
  String status=(String)row.get("status");
  if (((Timestamp)row.get("deadline")).toInstant().isAfter(Instant.now())) return;
  if (SagaLogic.active(status)) {
   var m=message(row);
   var decision=SagaLogic.cancel(m.mode());
   apply(m,decision,"TIMEOUT",true);
   history(id,"TIMEOUT",decision.status());
  } else if (SagaLogic.compensating(status)) {
   jdbc.update("update orders set status='MANUAL_INTERVENTION',updated_at=now() where id=?",id);
   history(id,"COMPENSATION_TIMEOUT","MANUAL_INTERVENTION");
  }
 }
 private void apply(Message original,SagaLogic.Decision decision,String reason,boolean resetDeadline) {
  jdbc.update("""
     update orders set status=?,cancel_mask=?,reason=?,updated_at=now(),
       deadline=case when ? then ? else deadline end where id=?
     """,decision.status(),decision.mask(),reason,resetDeadline,
     Timestamp.from(Instant.now().plusSeconds(compensationTimeout)),original.sagaId());
  for (Type command:decision.commands()) outbox.add(original.next(command));
 }
 private Map<String,Object> find(UUID id,boolean lock) {
  var rows=jdbc.queryForList("select * from orders where id=?"+(lock ? " for update":""),id);
  if (rows.isEmpty()) throw new ResponseStatusException(HttpStatus.NOT_FOUND,"Order not found");
  return rows.getFirst();
 }
 private Message message(Map<String,Object> row) {
  return json.readValue((String)row.get("message_json"),Message.class);
 }
 private void history(UUID id,String event,String status) {
  jdbc.update("insert into saga_history(saga_id,event,status) values (?,?,?)",id,event,status);
 }
}
