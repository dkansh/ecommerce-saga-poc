package dev.saga.shipping;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@EnableScheduling
@SpringBootApplication(scanBasePackages = {"dev.saga.messaging", "dev.saga.shipping"})
public class ShippingApplication {
    public static void main(String[] args) {
        SpringApplication.run(ShippingApplication.class, args);
    }
}
