package dev.saga.inventory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import dev.saga.messaging.Fault;
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
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import tools.jackson.databind.json.JsonMapper;

@Tag("integration")
class InventoryHandlerIntegrationTest {
    private static String schema;
    private static TimeZone originalTimeZone;
    private static JdbcTemplate jdbc;
    private static TransactionTemplate transaction;
    private InventoryHandler handler;

    @BeforeAll
    static void createSchema() {
        originalTimeZone = TimeZone.getDefault();
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        schema = "inventory_" + UUID.randomUUID().toString().replace("-", "");
        var admin = dataSource(databaseUrl());
        new JdbcTemplate(admin).execute("create schema " + schema);
        var service = dataSource(databaseUrl() + "?currentSchema=" + schema);
        Flyway.configure()
                .dataSource(service)
                .defaultSchema(schema)
                .schemas(schema)
                .locations("classpath:db/migration/common", "classpath:db/migration/own")
                .load()
                .migrate();
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
        jdbc.update("delete from inventory_reservation");
        jdbc.update("delete from outbox");
        jdbc.update("update inventory_stock set available = case sku when 'SKU-1' then 100 else 0 end");
        handler = new InventoryHandler(jdbc, new Outbox(jdbc, JsonMapper.builder().build()));
    }

    @Test
    void duplicateReserveWithNewMessageIdDoesNotRemoveStockTwice() {
        var sagaId = UUID.randomUUID();

        handle(message(sagaId, Type.RESERVE_INVENTORY, "SKU-1", 3));
        handle(message(sagaId, Type.RESERVE_INVENTORY, "SKU-1", 3));

        assertEquals(97, available("SKU-1"));
        assertEquals(1, jdbc.queryForObject(
                "select count(*) from inventory_reservation where saga_id = ?", Integer.class, sagaId));
        assertEquals(2, jdbc.queryForObject("select count(*) from outbox", Integer.class));
    }

    @Test
    void noStockCreatesRejectedReservationWithoutChangingStock() {
        var sagaId = UUID.randomUUID();

        handle(message(sagaId, Type.RESERVE_INVENTORY, "SOLD-OUT", 1));

        assertEquals("REJECTED", reservation(sagaId).get("state"));
        assertEquals(0, available("SOLD-OUT"));
        assertEquals("INVENTORY_REJECTED", latestType());
    }

    @Test
    void cancellationBeforeForwardCreatesTombstoneAndBlocksLateReserve() {
        var sagaId = UUID.randomUUID();

        handle(message(sagaId, Type.RELEASE_INVENTORY, "SKU-1", 4));
        handle(message(sagaId, Type.RESERVE_INVENTORY, "SKU-1", 4));

        assertEquals("RELEASED", reservation(sagaId).get("state"));
        assertEquals(100, available("SKU-1"));
        assertEquals("INVENTORY_REJECTED", latestType());
    }

    @Test
    void conflictingPayloadLeavesOriginalReservationUnchanged() {
        var sagaId = UUID.randomUUID();
        handle(message(sagaId, Type.RESERVE_INVENTORY, "SKU-1", 3));

        assertThrows(IllegalArgumentException.class,
                () -> handle(message(sagaId, Type.RESERVE_INVENTORY, "SKU-1", 4)));

        assertEquals(3, reservation(sagaId).get("quantity"));
        assertEquals(97, available("SKU-1"));
    }

    private void handle(Message message) {
        transaction.executeWithoutResult(ignored -> handler.handle(message));
    }

    private int available(String sku) {
        return jdbc.queryForObject("select available from inventory_stock where sku = ?", Integer.class, sku);
    }

    private Map<String, Object> reservation(UUID sagaId) {
        return jdbc.queryForMap("select * from inventory_reservation where saga_id = ?", sagaId);
    }

    private String latestType() {
        return jdbc.queryForObject("select payload::jsonb->>'type' from outbox order by sequence desc limit 1", String.class);
    }

    private static Message message(UUID sagaId, Type type, String sku, int quantity) {
        return new Message(1, UUID.randomUUID(), sagaId, Mode.ORCHESTRATION, type,
                sku, quantity, 2_500, Fault.NONE);
    }

    private static DataSource dataSource(String url) {
        return new DriverManagerDataSource(url, "saga_test", "saga_test");
    }

    private static String databaseUrl() {
        return System.getenv().getOrDefault("TEST_DB_URL", "jdbc:postgresql://localhost:55432/saga_test");
    }
}
