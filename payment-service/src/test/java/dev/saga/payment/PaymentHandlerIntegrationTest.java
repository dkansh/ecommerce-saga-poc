package dev.saga.payment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import dev.saga.messaging.Fault;
import dev.saga.messaging.Faults;
import dev.saga.messaging.Message;
import dev.saga.messaging.Mode;
import dev.saga.messaging.Outbox;
import dev.saga.messaging.Type;
import java.util.Map;
import java.util.TimeZone;
import java.util.UUID;
import javax.sql.DataSource;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.transaction.support.TransactionTemplate;
import tools.jackson.databind.json.JsonMapper;

@Tag("integration")
class PaymentHandlerIntegrationTest {
    private static String schema;
    private static TimeZone originalTimeZone;
    private static JdbcTemplate jdbc;
    private static TransactionTemplate transaction;
    private PaymentHandler handler;

    @BeforeAll
    static void createSchema() {
        originalTimeZone = TimeZone.getDefault();
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        schema = "payment_" + UUID.randomUUID().toString().replace("-", "");
        var admin = dataSource(databaseUrl());
        new JdbcTemplate(admin).execute("create schema " + schema);
        var service = dataSource(databaseUrl() + "?currentSchema=" + schema);
        Flyway.configure().dataSource(service).defaultSchema(schema).schemas(schema)
                .locations("classpath:db/migration/common", "classpath:db/migration/own")
                .load().migrate();
        jdbc = new JdbcTemplate(service);
        transaction = new TransactionTemplate(new DataSourceTransactionManager(service));
    }

    @AfterAll
    static void dropSchema() {
        new JdbcTemplate(dataSource(databaseUrl()))
                .execute("drop schema " + schema + " cascade");
        TimeZone.setDefault(originalTimeZone);
    }

    @BeforeEach
    void reset() {
        jdbc.update("delete from payment");
        jdbc.update("delete from outbox");
        handler = new PaymentHandler(jdbc, new Outbox(jdbc, JsonMapper.builder().build()),
                new Faults(jdbc, false), new SimulatedPaymentProvider(true));
    }

    @Test
    void duplicateRefundWithNewMessageIdDoesNotChangeTheCompletedRefund() {
        var sagaId = UUID.randomUUID();
        handle(message(sagaId, Type.CHARGE_PAYMENT, 2_500, Fault.NONE));

        handle(message(sagaId, Type.REFUND_PAYMENT, 2_500, Fault.NONE));
        handle(message(sagaId, Type.REFUND_PAYMENT, 2_500, Fault.NONE));

        assertEquals("REFUNDED", payment(sagaId).get("state"));
        assertEquals(1, jdbc.queryForObject("select count(*) from payment where saga_id = ?", Integer.class, sagaId));
        assertEquals("PAYMENT_REFUNDED", latestType());
    }

    @Test
    void cancellationBeforeForwardCreatesTombstoneAndBlocksLateCharge() {
        var sagaId = UUID.randomUUID();

        handle(message(sagaId, Type.REFUND_PAYMENT, 2_500, Fault.NONE));
        handle(message(sagaId, Type.CHARGE_PAYMENT, 2_500, Fault.NONE));

        assertEquals("REFUNDED", payment(sagaId).get("state"));
        assertEquals("PAYMENT_REJECTED", latestType());
    }

    @Test
    void declinedPaymentIsRecordedAsRejected() {
        var sagaId = UUID.randomUUID();

        handle(message(sagaId, Type.CHARGE_PAYMENT, 2_500, Fault.PAYMENT_DECLINED));

        assertEquals("REJECTED", payment(sagaId).get("state"));
        assertEquals("PAYMENT_REJECTED", latestType());
    }

    @Test
    void conflictingAmountLeavesOriginalChargeUnchanged() {
        var sagaId = UUID.randomUUID();
        handle(message(sagaId, Type.CHARGE_PAYMENT, 2_500, Fault.NONE));

        assertThrows(IllegalArgumentException.class,
                () -> handle(message(sagaId, Type.CHARGE_PAYMENT, 2_600, Fault.NONE)));

        assertEquals(2_500L, ((Number) payment(sagaId).get("amount_cents")).longValue());
        assertEquals("CHARGED", payment(sagaId).get("state"));
    }

    private void handle(Message message) {
        transaction.executeWithoutResult(ignored -> handler.handle(message));
    }

    private Map<String, Object> payment(UUID sagaId) {
        return jdbc.queryForMap("select * from payment where saga_id = ?", sagaId);
    }

    private String latestType() {
        return jdbc.queryForObject("select payload::jsonb->>'type' from outbox order by sequence desc limit 1", String.class);
    }

    private static Message message(UUID sagaId, Type type, long amount, Fault fault) {
        return new Message(1, UUID.randomUUID(), sagaId, Mode.ORCHESTRATION, type,
                "SKU-1", 2, amount, fault);
    }

    private static DataSource dataSource(String url) {
        return new DriverManagerDataSource(url, "saga_test", "saga_test");
    }

    private static String databaseUrl() {
        return System.getenv().getOrDefault("TEST_DB_URL", "jdbc:postgresql://localhost:55432/saga_test");
    }
}
