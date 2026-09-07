package dev.saga.messaging;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
public class Quarantine {
 private final JdbcTemplate jdbc;
 private final Outbox outbox;
 private final String service;
 public Quarantine(JdbcTemplate jdbc, Outbox outbox, @Value("${spring.application.name}") String service) {
  this.jdbc=jdbc; this.outbox=outbox; this.service=service;
 }
 @Transactional
 public void save(ConsumerRecord<?,?> record, Exception failure) {
  String payload=String.valueOf(record.value());
  int inserted=jdbc.update("""
      insert into failed_message(topic,partition_id,offset_id,message_key,payload,error)
      values (?,?,?,?,?,?) on conflict do nothing
      """,record.topic(),record.partition(),record.offset(),record.key(),payload,failure.toString());
  if (inserted==1) outbox.raw(service+".DLT",String.valueOf(record.key()),payload);
 }
}
