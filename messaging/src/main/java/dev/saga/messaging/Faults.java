package dev.saga.messaging;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

@Component
public class Faults {
 private final JdbcTemplate jdbc;
 private final boolean enabled;
 public Faults(JdbcTemplate jdbc, @Value("${demo.enabled:false}") boolean enabled) {
  this.jdbc=jdbc; this.enabled=enabled;
 }
 @Transactional(propagation=Propagation.REQUIRES_NEW, noRollbackFor=InjectedFailure.class)
 public void check(Message m, Fault expected, boolean once) {
  if (!enabled || m.fault()!=expected) return;
  if (Boolean.TRUE.equals(jdbc.queryForObject("select exists(select 1 from fault_unblocked where saga_id=?)", Boolean.class, m.sagaId()))) return;
  int attempt=jdbc.queryForObject("""
      insert into fault_attempt(saga_id,fault,attempts) values (?,?,1)
      on conflict(saga_id,fault) do update set attempts=fault_attempt.attempts+1
      returning attempts
      """, Integer.class, m.sagaId(), expected.name());
  // Commit the attempt even when the caller's business transaction rolls back.
  if (!once || attempt==1) throw new InjectedFailure(expected.name());
 }
 public static class InjectedFailure extends RuntimeException {
  public InjectedFailure(String fault) { super("Demo provider unavailable: "+fault); }
 }
}
