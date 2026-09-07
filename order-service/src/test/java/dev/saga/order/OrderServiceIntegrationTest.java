package dev.saga.order;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.saga.messaging.Fault;
import dev.saga.messaging.Message;
import dev.saga.messaging.Mode;
import dev.saga.messaging.Outbox;
import dev.saga.messaging.Type;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.TimeZone;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.postgresql.ds.PGSimpleDataSource;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.JdbcTransactionManager;
import org.springframework.transaction.TransactionManager;
import org.springframework.transaction.annotation.AnnotationTransactionAttributeSource;
import org.springframework.transaction.interceptor.TransactionInterceptor;
import org.springframework.web.server.ResponseStatusException;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class OrderServiceIntegrationTest {
 private static final String DATABASE_URL = environment(
     "TEST_DB_URL", "jdbc:postgresql://localhost:55432/saga_test");
 private static final String DATABASE_USER = environment("TEST_DB_USER", "saga_test");
 private static final String DATABASE_PASSWORD = environment("TEST_DB_PASSWORD", "saga_test");

 private final ObjectMapper json = JsonMapper.builder().build();
 private TimeZone originalTimeZone;
 private String schema;
 private PGSimpleDataSource dataSource;
 private JdbcTemplate jdbc;
 private TransactionInterceptor transactionAdvice;
 private OrderService orders;

 @BeforeAll
 void migrateRandomSchema() {
  originalTimeZone = TimeZone.getDefault();
  TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
  schema = "order_it_" + UUID.randomUUID().toString().replace("-", "");
  dataSource = dataSource(schema);
  Flyway.configure()
      .dataSource(dataSource)
      .locations("classpath:db/migration/common", "classpath:db/migration/own")
      .schemas(schema)
      .defaultSchema(schema)
      .load()
      .migrate();

  jdbc = new JdbcTemplate(dataSource);
  TransactionManager transactionManager = new JdbcTransactionManager(dataSource);
  transactionAdvice = new TransactionInterceptor(
      transactionManager, new AnnotationTransactionAttributeSource());
  var outbox = transactionalProxy(new Outbox(jdbc, json), Outbox.class);
  orders = transactionalProxy(new OrderService(jdbc, outbox, json, 60, 60, false), OrderService.class);
 }

 @AfterAll
 void dropRandomSchema() throws SQLException {
  try {
   if (schema == null) return;
   try (var connection = dataSource(null).getConnection();
        var statement = connection.createStatement()) {
    statement.execute("drop schema if exists " + schema + " cascade");
   }
  } finally {
   if (originalTimeZone != null) TimeZone.setDefault(originalTimeZone);
  }
 }

 @BeforeEach
 void clearRows() {
  jdbc.update("delete from saga_history");
  jdbc.update("delete from orders");
  jdbc.update("delete from outbox");
  jdbc.update("delete from inbox");
  jdbc.update("delete from fault_attempt");
  jdbc.update("delete from fault_unblocked");
 }

 @Test
 void sameIdempotencyKeyAndRequestReturnsTheOriginalOrder() {
  var request = request(2);

  var first = orders.create("same-request", request);
  var second = orders.create("same-request", request);

  assertThat(second.get("id")).isEqualTo(first.get("id"));
  assertThat(rowCount("orders")).isEqualTo(1);
  assertThat(rowCount("outbox")).isEqualTo(1);
  assertThat(rowCount("saga_history")).isEqualTo(1);
 }

 @Test
 void sameIdempotencyKeyWithChangedPayloadReturnsConflict() {
  orders.create("changed-request", request(2));

  assertThatThrownBy(() -> orders.create("changed-request", request(3)))
      .isInstanceOfSatisfying(ResponseStatusException.class,
          error -> assertThat(error.getStatusCode()).isEqualTo(HttpStatus.CONFLICT));

  assertThat(rowCount("orders")).isEqualTo(1);
  assertThat(rowCount("outbox")).isEqualTo(1);
 }

 @Test
 void concurrentRequestsWithTheSameIdempotencyKeyCreateOneOrderAndOutboxMessage()
     throws Exception {
  var request = request(2);
  var ready = new CountDownLatch(2);
  var start = new CountDownLatch(1);

  try (var workers = Executors.newFixedThreadPool(2)) {
   var first = workers.submit(() -> createAfterSignal("concurrent-request", request, ready, start));
   var second = workers.submit(() -> createAfterSignal("concurrent-request", request, ready, start));
   assertThat(ready.await(5, SECONDS)).isTrue();
   start.countDown();

   assertThat(second.get(10, SECONDS)).isEqualTo(first.get(10, SECONDS));
  }

  assertThat(rowCount("orders")).isEqualTo(1);
  assertThat(rowCount("outbox")).isEqualTo(1);
  assertThat(rowCount("saga_history")).isEqualTo(1);
 }

 @Test
 void persistedDeadlineStartsCancellationWhenExpired() {
  var created = orders.create("expired-order", request(2));
  var id = (UUID)created.get("id");
  assertThat(((Timestamp)created.get("deadline")).toInstant()).isAfter(Instant.now());

  jdbc.update("update orders set deadline=now()-interval '1 second' where id=?", id);
  orders.expire(id);

  var expired = orders.get(id);
  assertThat(expired.get("status")).isEqualTo("CANCELLING_SHIPPING");
  assertThat(expired.get("reason")).isEqualTo("TIMEOUT");
  assertThat(((Timestamp)expired.get("deadline")).toInstant()).isAfter(Instant.now());
  assertThat(outboxMessages()).extracting(Message::type)
      .containsExactly(Type.RESERVE_INVENTORY, Type.CANCEL_SHIPMENT);
 }

 @Test
 void compensationExpiryRequiresManualInterventionAndRetryEmitsFreshCommand() {
  var created = orders.create("retry-compensation", request(2));
  var id = (UUID)created.get("id");
  orders.cancel(id);
  var firstCancellation = outboxMessages().getLast();

  jdbc.update("update orders set deadline=now()-interval '1 second' where id=?", id);
  orders.expire(id);

  assertThat(orders.get(id).get("status")).isEqualTo("MANUAL_INTERVENTION");
  assertThat(rowCount("outbox")).isEqualTo(2);

  var retried = orders.retry(id);
  var messages = outboxMessages();
  var secondCancellation = messages.getLast();

  assertThat(retried.get("status")).isEqualTo("CANCELLING_SHIPPING");
  assertThat(messages).extracting(Message::type)
      .containsExactly(Type.RESERVE_INVENTORY, Type.CANCEL_SHIPMENT, Type.CANCEL_SHIPMENT);
  assertThat(secondCancellation.id()).isNotEqualTo(firstCancellation.id());
 }

 private UUID createAfterSignal(
     String key, CreateOrder request, CountDownLatch ready, CountDownLatch start) throws Exception {
  ready.countDown();
  assertThat(start.await(5, SECONDS)).isTrue();
  return (UUID)orders.create(key, request).get("id");
 }

 private CreateOrder request(int quantity) {
  return new CreateOrder(Mode.ORCHESTRATION, "SKU-1", quantity, 2500, Fault.NONE);
 }

 private List<Message> outboxMessages() {
  return jdbc.query(
      "select payload from outbox order by sequence",
      (result, row) -> json.readValue(result.getString("payload"), Message.class));
 }

 private int rowCount(String table) {
  return jdbc.queryForObject("select count(*) from " + table, Integer.class);
 }

 private PGSimpleDataSource dataSource(String currentSchema) {
  var source = new PGSimpleDataSource();
  source.setURL(DATABASE_URL);
  source.setUser(DATABASE_USER);
  source.setPassword(DATABASE_PASSWORD);
  if (currentSchema != null) source.setCurrentSchema(currentSchema);
  return source;
 }

 private static String environment(String name, String defaultValue) {
  var value = System.getenv(name);
  return value == null || value.isBlank() ? defaultValue : value;
 }

 private <T> T transactionalProxy(T target, Class<T> type) {
  var proxyFactory = new ProxyFactory(target);
  proxyFactory.setProxyTargetClass(true);
  proxyFactory.addAdvice(transactionAdvice);
  return type.cast(proxyFactory.getProxy());
 }
}
