package dev.saga.order;
import dev.saga.messaging.*;
import jakarta.validation.constraints.*;
public record CreateOrder(
 @NotNull Mode mode,
 @NotBlank @Pattern(regexp="[A-Z0-9-]{1,40}") String sku,
 @Min(1) @Max(1000) int quantity,
 @Min(1) @Max(1_000_000_000L) long amountCents,
 Fault fault) {
 public CreateOrder { if (fault==null) fault=Fault.NONE; }
}
