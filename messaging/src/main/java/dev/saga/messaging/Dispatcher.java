package dev.saga.messaging;
import java.util.List;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.ObjectMapper;

@Component
public class Dispatcher {
 private final JdbcTemplate jdbc;
 private final ObjectMapper json;
 private final List<MessageHandler> handlers;
 public Dispatcher(JdbcTemplate jdbc, ObjectMapper json, List<MessageHandler> handlers) {
  this.jdbc=jdbc; this.json=json; this.handlers=handlers;
 }
 @Transactional
 public void process(String payload, String key) {
  Message m=json.readValue(payload, Message.class);
  m.validate();
  if (!m.sagaId().toString().equals(key)) throw new IllegalArgumentException("Kafka key must equal sagaId");
  var interested=handlers.stream().filter(h->h.accepts(m)).toList();
  if (interested.isEmpty()) return;
  if (jdbc.update("insert into inbox(message_id) values (?) on conflict do nothing",m.id())==0) return;
  // Serializes different event IDs for one saga, including cancellation-before-forward races.
  jdbc.queryForList("select pg_advisory_xact_lock(hashtextextended(?,0))",m.sagaId().toString());
  try (var ignored=MDC.putCloseable("sagaId",m.sagaId().toString())) {
   for (var handler:interested) handler.handle(m);
   LoggerFactory.getLogger(Dispatcher.class).info("Processed type={} messageId={} mode={}",m.type(),m.id(),m.mode());
  }
 }
}
