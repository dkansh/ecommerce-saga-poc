package dev.saga.payment;

import java.util.Map;
import java.util.UUID;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/payments")
public class PaymentController {
    private final JdbcTemplate jdbc;

    public PaymentController(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    @GetMapping("/{sagaId}")
    public ResponseEntity<Map<String, Object>> payment(@PathVariable UUID sagaId) {
        var rows = jdbc.queryForList("select * from payment where saga_id = ?", sagaId);
        return rows.isEmpty() ? ResponseEntity.notFound().build() : ResponseEntity.ok(rows.getFirst());
    }
}
