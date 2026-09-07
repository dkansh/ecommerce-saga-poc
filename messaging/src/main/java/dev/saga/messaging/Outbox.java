package dev.saga.messaging;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.ObjectMapper;

@Component
public class Outbox {
 public static final String TOPIC = "ecommerce.saga";
 private final JdbcTemplate jdbc;
 private final ObjectMapper json;
 public Outbox(JdbcTemplate jdbc, ObjectMapper json) { this.jdbc=jdbc; this.json=json; }
 @Transactional(propagation=Propagation.MANDATORY)
 public void add(Message message) {
  message.validate();
  raw(TOPIC, message.sagaId().toString(), json.writeValueAsString(message));
 }
 @Transactional(propagation=Propagation.MANDATORY)
 public void raw(String topic, String key, String payload) {
  jdbc.update("insert into outbox(topic,message_key,payload) values (?,?,?)", topic, key == null ? "" : key, payload);
 }
}
