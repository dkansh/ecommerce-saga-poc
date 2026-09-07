package dev.saga.order;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;
@SpringBootApplication(scanBasePackages={"dev.saga.order","dev.saga.messaging"})
@EnableScheduling
public class OrderApplication {
 public static void main(String[] args) { SpringApplication.run(OrderApplication.class,args); }
}
