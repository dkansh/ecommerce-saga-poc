package dev.saga.inventory;

import java.util.Map;
import java.util.UUID;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/inventory")
public class InventoryController {
    private final JdbcTemplate jdbc;

    public InventoryController(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    @GetMapping("/{sagaId}")
    public ResponseEntity<Map<String, Object>> reservation(@PathVariable UUID sagaId) {
        return one("select * from inventory_reservation where saga_id = ?", sagaId);
    }

    @GetMapping("/stock/{sku}")
    public ResponseEntity<Map<String, Object>> stock(@PathVariable String sku) {
        return one("select sku, available from inventory_stock where sku = ?", sku);
    }

    private ResponseEntity<Map<String, Object>> one(String sql, Object value) {
        var rows = jdbc.queryForList(sql, value);
        return rows.isEmpty() ? ResponseEntity.notFound().build() : ResponseEntity.ok(rows.getFirst());
    }
}
