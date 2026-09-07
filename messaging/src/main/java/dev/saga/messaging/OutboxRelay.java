package dev.saga.messaging;
import java.time.Instant;
import java.util.concurrent.TimeUnit;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

@Component
public class OutboxRelay {
 private final JdbcTemplate jdbc;
 private final KafkaTemplate<String,String> kafka;
 private final TransactionTemplate tx;
 private Instant nextAttempt=Instant.EPOCH;
 private int failures;
 public OutboxRelay(JdbcTemplate jdbc, KafkaTemplate<String,String> kafka, TransactionTemplate tx) {
  this.jdbc=jdbc; this.kafka=kafka; this.tx=tx;
 }
 @Scheduled(fixedDelayString="${saga.relay-delay-ms:250}")
 public void publish() {
  if (Instant.now().isBefore(nextAttempt)) return;
  try {
   tx.executeWithoutResult(status->{
    if (!Boolean.TRUE.equals(jdbc.queryForObject("select pg_try_advisory_xact_lock(48291027)",Boolean.class))) return;
    // One publisher per service database preserves insertion order, even across replicas.
    var rows=jdbc.queryForList("select * from outbox where sent_at is null order by sequence limit 20");
    for (var row:rows) {
     try {
      kafka.send((String)row.get("topic"),(String)row.get("message_key"),(String)row.get("payload")).get(5,TimeUnit.SECONDS);
     } catch (InterruptedException e) {
      Thread.currentThread().interrupt(); throw new IllegalStateException("Publisher interrupted",e);
     } catch (Exception e) { throw new IllegalStateException("Kafka acknowledgement unavailable",e); }
     jdbc.update("update outbox set sent_at=now() where sequence=?",row.get("sequence"));
    }
   });
   failures=0;
  } catch (RuntimeException e) {
   long delay=Math.min(30,1L << Math.min(5,failures++));
   nextAttempt=Instant.now().plusSeconds(delay);
   LoggerFactory.getLogger(getClass()).warn("Outbox retained; retry in {}s: {}",delay,e.getMessage());
  }
 }
}
