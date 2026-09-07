package dev.saga.messaging;
public interface MessageHandler {
 boolean accepts(Message message);
 void handle(Message message);
}
