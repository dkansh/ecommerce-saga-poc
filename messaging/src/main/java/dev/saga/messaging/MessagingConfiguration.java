package dev.saga.messaging;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class MessagingConfiguration {
 @Bean NewTopic sagaTopic() { return TopicBuilder.name(Outbox.TOPIC).partitions(3).replicas(1).build(); }
 @Bean NewTopic deadLetterTopic(@Value("${spring.application.name}") String service) {
  return TopicBuilder.name(service+".DLT").partitions(3).replicas(1).build();
 }
 @Bean DefaultErrorHandler errorHandler(Quarantine quarantine) {
  var handler=new DefaultErrorHandler(quarantine::save,new FixedBackOff(1000L,2L));
  handler.addNotRetryableExceptions(IllegalArgumentException.class);
  return handler;
 }
 @Bean Incoming incoming(Dispatcher dispatcher) { return new Incoming(dispatcher); }
 public static class Incoming {
  private final Dispatcher dispatcher;
  Incoming(Dispatcher dispatcher) { this.dispatcher=dispatcher; }
  @KafkaListener(topics=Outbox.TOPIC)
  public void receive(String payload, @Header(KafkaHeaders.RECEIVED_KEY) String key) {
   dispatcher.process(payload,key);
  }
 }
}
