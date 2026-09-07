package dev.saga.shipping;

import java.util.Map;
import java.util.UUID;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/shipments")
public class ShippingController {
    private final JdbcTemplate jdbc;

    public ShippingController(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    @GetMapping("/{sagaId}")
    public ResponseEntity<Map<String, Object>> shipment(@PathVariable UUID sagaId) {
        var rows = jdbc.queryForList("select * from shipment where saga_id = ?", sagaId);
        return rows.isEmpty() ? ResponseEntity.notFound().build() : ResponseEntity.ok(rows.getFirst());
    }
}
