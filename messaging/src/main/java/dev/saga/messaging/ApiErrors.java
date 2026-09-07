package dev.saga.messaging;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
@RestControllerAdvice
public class ApiErrors {
 @ExceptionHandler(IllegalArgumentException.class)
 public ProblemDetail invalid(IllegalArgumentException e) {
  return ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST,e.getMessage());
 }
}
