package dev.saga.order;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
@Component
public class OrderWatchdog {
 private final JdbcTemplate jdbc;
 private final OrderService orders;
 public OrderWatchdog(JdbcTemplate jdbc,OrderService orders) { this.jdbc=jdbc; this.orders=orders; }
 @Scheduled(fixedDelay=1000)
 public void expire() {
  // Each order uses its own short transaction; row re-check makes concurrent watchdogs safe.
  var ids=jdbc.queryForList("""
     select id from orders where deadline<now()
     and status not in ('COMPLETED','CANCELLED','MANUAL_INTERVENTION')
     order by deadline limit 100
     """,UUID.class);
  for (UUID id:ids) orders.expire(id);
 }
}
