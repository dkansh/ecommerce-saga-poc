package dev.saga.messaging;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.transaction.annotation.Transactional;

@RestController
@RequestMapping("/ops")
@ConditionalOnProperty(name="demo.enabled",havingValue="true")
public class OperationsController {
 private final JdbcTemplate jdbc;
 private final Outbox outbox;
 private final String token;
 public OperationsController(JdbcTemplate jdbc, Outbox outbox, @Value("${demo.token}") String token) {
  this.jdbc=jdbc; this.outbox=outbox; this.token=token;
 }
 private void authorize(String supplied) {
  if (!java.security.MessageDigest.isEqual(token.getBytes(java.nio.charset.StandardCharsets.UTF_8),
      supplied.getBytes(java.nio.charset.StandardCharsets.UTF_8)))
   throw new ResponseStatusException(HttpStatus.FORBIDDEN,"Invalid demo token");
 }
 @GetMapping("/failures")
 public List<Map<String,Object>> failures(@RequestHeader("X-Demo-Token") String supplied) {
  authorize(supplied);
  return jdbc.queryForList("select * from failed_message order by id desc limit 100");
 }
 @PostMapping("/unblock/{sagaId}")
 public void unblock(@PathVariable UUID sagaId,@RequestHeader("X-Demo-Token") String supplied) {
  authorize(supplied); jdbc.update("insert into fault_unblocked(saga_id) values (?) on conflict do nothing",sagaId);
 }
 @PostMapping("/replay/{id}")
 @Transactional
 public void replay(@PathVariable long id,@RequestHeader("X-Demo-Token") String supplied) {
  authorize(supplied);
  var rows=jdbc.queryForList("select * from failed_message where id=? for update",id);
  if (rows.isEmpty()) throw new ResponseStatusException(HttpStatus.NOT_FOUND,"Unknown failure");
  var row=rows.getFirst();
  if (row.get("replayed_at")!=null) throw new ResponseStatusException(HttpStatus.CONFLICT,"Already replayed; inspect any new failure");
  outbox.raw((String)row.get("topic"),(String)row.get("message_key"),(String)row.get("payload"));
  jdbc.update("update failed_message set replayed_at=now() where id=?",id);
 }
 @PostMapping("/publish")
 @Transactional
 public void publish(@RequestBody Message message,@RequestHeader("X-Demo-Token") String supplied) {
  authorize(supplied); message.validate(); outbox.add(message);
 }
 @GetMapping("/outbox")
 public Map<String,Object> outbox(@RequestHeader("X-Demo-Token") String supplied) {
  authorize(supplied);
  return jdbc.queryForMap("select count(*) filter(where sent_at is null) as pending, count(*) as total from outbox");
 }
}
