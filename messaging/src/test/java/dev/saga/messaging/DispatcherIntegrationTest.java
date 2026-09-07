package dev.saga.messaging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.sql.SQLException;
import java.util.List;
import java.util.TimeZone;
import java.util.UUID;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.postgresql.ds.PGSimpleDataSource;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.JdbcTransactionManager;
import org.springframework.transaction.TransactionManager;
import org.springframework.transaction.annotation.AnnotationTransactionAttributeSource;
import org.springframework.transaction.interceptor.TransactionInterceptor;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class DispatcherIntegrationTest {
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
 private Outbox outbox;
 private Faults faults;

 @BeforeAll
 void migrateRandomSchema() {
  originalTimeZone = TimeZone.getDefault();
  TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
  schema = "messaging_it_" + UUID.randomUUID().toString().replace("-", "");
  dataSource = dataSource(schema);
  Flyway.configure()
      .dataSource(dataSource)
      .locations("classpath:db/migration/common")
      .schemas(schema)
      .defaultSchema(schema)
      .load()
      .migrate();

  jdbc = new JdbcTemplate(dataSource);
  jdbc.execute("""
      create table test_ledger (
       sequence bigint generated always as identity primary key,
       message_id uuid not null,
       saga_id uuid not null,
       handled_type text not null
      )
      """);

  TransactionManager transactionManager = new JdbcTransactionManager(dataSource);
  transactionAdvice = new TransactionInterceptor(
      transactionManager, new AnnotationTransactionAttributeSource());
  outbox = transactionalProxy(new Outbox(jdbc, json), Outbox.class);
  faults = transactionalProxy(new Faults(jdbc, true), Faults.class);
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
  jdbc.update("delete from test_ledger");
  jdbc.update("delete from outbox");
  jdbc.update("delete from inbox");
  jdbc.update("delete from fault_attempt");
  jdbc.update("delete from fault_unblocked");
 }

 @Test
 void duplicateMessageIdRunsBusinessHandlerOnlyOnce() {
  var handler = new LedgerHandler(false, false);
  var dispatcher = dispatcher(handler);
  var message = message(1);
  var payload = json.writeValueAsString(message);

  dispatcher.process(payload, message.sagaId().toString());
  dispatcher.process(payload, message.sagaId().toString());

  assertThat(rowCount("test_ledger")).isEqualTo(1);
  assertThat(rowCount("inbox")).isEqualTo(1);
 }

 @Test
 void operatorRouteBindsSagaIdAndPersistsUnblock() throws Exception {
  var id=UUID.randomUUID();
  var mvc=org.springframework.test.web.servlet.setup.MockMvcBuilders
      .standaloneSetup(new OperationsController(jdbc,outbox,"local-demo-token")).build();
  mvc.perform(org.springframework.test.web.servlet.request.MockMvcRequestBuilders
      .post("/ops/unblock/"+id).header("X-Demo-Token","local-demo-token"))
      .andExpect(org.springframework.test.web.servlet.result.MockMvcResultMatchers.status().isOk());
  assertThat(jdbc.queryForObject("select count(*) from fault_unblocked where saga_id=?",Integer.class,id)).isEqualTo(1);
 }

 @Test
 void failedHandlerRollsBackInboxSoSameMessageIdCanBeRetried() {
  var dispatcher = dispatcher(new FaultOnceHandler());
  var message = message(1, Fault.PAYMENT_TRANSIENT);
  var payload = json.writeValueAsString(message);

  assertThatThrownBy(() -> dispatcher.process(payload, message.sagaId().toString()))
      .isInstanceOf(Faults.InjectedFailure.class);
  assertThat(rowCount("test_ledger")).isZero();
  assertThat(rowCount("inbox")).isZero();
  assertThat(faultAttempts(message)).isEqualTo(1);

  dispatcher.process(payload, message.sagaId().toString());

  assertThat(rowCount("test_ledger")).isEqualTo(1);
  assertThat(rowCount("inbox")).isEqualTo(1);
  assertThat(faultAttempts(message)).isEqualTo(2);
 }

 @Test
 void outboxAndBusinessRowsRollBackInTheSameTransaction() {
  var handler = new LedgerHandler(true, true);
  var dispatcher = dispatcher(handler);
  var message = message(1);

  assertThatThrownBy(() -> dispatcher.process(
      json.writeValueAsString(message), message.sagaId().toString()))
      .isInstanceOf(HandlerFailure.class);

  assertThat(rowCount("test_ledger")).isZero();
  assertThat(rowCount("outbox")).isZero();
  assertThat(rowCount("inbox")).isZero();
 }

 @Test
 void rejectsKafkaKeyThatDoesNotMatchSagaIdBeforeWritingAnything() {
  var dispatcher = dispatcher(new LedgerHandler(false, false));
  var message = message(1);

  assertThatThrownBy(() -> dispatcher.process(
      json.writeValueAsString(message), UUID.randomUUID().toString()))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessageContaining("Kafka key");

  assertThat(rowCount("test_ledger")).isZero();
  assertThat(rowCount("inbox")).isZero();
 }

 @Test
 void rejectsUnknownEnvelopeVersionBeforeWritingAnything() {
  var dispatcher = dispatcher(new LedgerHandler(false, false));
  var message = message(2);

  assertThatThrownBy(() -> dispatcher.process(
      json.writeValueAsString(message), message.sagaId().toString()))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessageContaining("version");

  assertThat(rowCount("test_ledger")).isZero();
  assertThat(rowCount("inbox")).isZero();
 }

 private Dispatcher dispatcher(MessageHandler handler) {
  return transactionalProxy(new Dispatcher(jdbc, json, List.of(handler)), Dispatcher.class);
 }

 private int rowCount(String table) {
  return jdbc.queryForObject("select count(*) from " + table, Integer.class);
 }

 private Message message(int version) {
  return message(version, Fault.NONE);
 }

 private Message message(int version, Fault fault) {
  return new Message(
      version,
      UUID.randomUUID(),
      UUID.randomUUID(),
      Mode.ORCHESTRATION,
      Type.ORDER_CREATED,
      "SKU-1",
      2,
      2500,
      fault);
 }

 private int faultAttempts(Message message) {
  return jdbc.queryForObject(
      "select attempts from fault_attempt where saga_id=? and fault=?",
      Integer.class, message.sagaId(), message.fault().name());
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

 private final class LedgerHandler implements MessageHandler {
  private boolean failAfterWrite;
  private final boolean emitOutbox;

  private LedgerHandler(boolean failAfterWrite, boolean emitOutbox) {
   this.failAfterWrite = failAfterWrite;
   this.emitOutbox = emitOutbox;
  }

  @Override
  public boolean accepts(Message message) {
   return message.type() == Type.ORDER_CREATED;
  }

  @Override
  public void handle(Message message) {
   jdbc.update(
       "insert into test_ledger(message_id,saga_id,handled_type) values (?,?,?)",
       message.id(), message.sagaId(), message.type().name());
   if (emitOutbox) outbox.add(message.next(Type.RESERVE_INVENTORY));
   if (failAfterWrite) throw new HandlerFailure();
  }
 }

 private final class FaultOnceHandler implements MessageHandler {
  @Override
  public boolean accepts(Message message) {
   return message.type() == Type.ORDER_CREATED;
  }

  @Override
  public void handle(Message message) {
   jdbc.update(
       "insert into test_ledger(message_id,saga_id,handled_type) values (?,?,?)",
       message.id(), message.sagaId(), message.type().name());
   faults.check(message, Fault.PAYMENT_TRANSIENT, true);
  }
 }

 private static final class HandlerFailure extends RuntimeException { }
}
