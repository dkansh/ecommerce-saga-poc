package dev.saga.order;
import java.net.URI;
import java.util.*;
import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/orders")
public class OrderController {
 private final OrderService orders;
 public OrderController(OrderService orders) { this.orders=orders; }
 @PostMapping
 public ResponseEntity<Map<String,Object>> create(@RequestHeader("Idempotency-Key") String key,
     @Valid @RequestBody CreateOrder request) {
  var result=orders.create(key,request);
  return ResponseEntity.accepted().location(URI.create("/orders/"+result.get("id"))).body(result);
 }
 @GetMapping("/{id}") public Map<String,Object> get(@PathVariable UUID id) { return orders.get(id); }
 @GetMapping("/{id}/history") public List<Map<String,Object>> history(@PathVariable UUID id) { return orders.history(id); }
 @PostMapping("/{id}/cancel") public ResponseEntity<Map<String,Object>> cancel(@PathVariable UUID id) {
  return ResponseEntity.accepted().body(orders.cancel(id));
 }
 @PostMapping("/{id}/retry") public ResponseEntity<Map<String,Object>> retry(@PathVariable UUID id) {
  return ResponseEntity.accepted().body(orders.retry(id));
 }
}
