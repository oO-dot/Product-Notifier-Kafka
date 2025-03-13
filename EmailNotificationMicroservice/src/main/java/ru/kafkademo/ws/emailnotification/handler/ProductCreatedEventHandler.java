package ru.kafkademo.ws.emailnotification.handler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestTemplate;
import ru.kafkademo.ws.core.ProductCreatedEvent;
import ru.kafkademo.ws.emailnotification.exception.NonRetryableException;
import ru.kafkademo.ws.emailnotification.exception.RetryableException;

import java.rmi.AccessException;

@Component
@KafkaListener(topics = "product-created-events-topic")
public class ProductCreatedEventHandler {

    private RestTemplate restTemplate;
    private final Logger LOGGER = LoggerFactory.getLogger(this.getClass());

    public ProductCreatedEventHandler(RestTemplate restTemplate) {
        this.restTemplate = restTemplate;
    }

    @KafkaHandler
    public void handle(ProductCreatedEvent productCreatedEvent) {
        LOGGER.info("Received event: {}", productCreatedEvent.getTitle());

        // Эмуляция микросервиса (тестовый код)
        String url = "http://localhost:8090";
        try {
            ResponseEntity<String> response = restTemplate.exchange(url, HttpMethod.GET, null, String.class);
            if (response.getStatusCode().value() == HttpStatus.OK.value()) {
                LOGGER.info("Received response: {}", response.getBody());
            }
        } catch (ResourceAccessException e) { // если ресурс не доступен пробуем несколько раз retry делать мало ли поднимется и все будет ок
            LOGGER.error(e.getMessage());
            throw new RetryableException(e);
        } catch (HttpServerErrorException e) { // если внутренняя ошибка то не ретраим
            LOGGER.error(e.getMessage());
            throw new RetryableException(e);
        } catch (Exception e) { // если exception в коде то тоже считаем что это NonRetryable (повторять есть смысл когда ресурс недоступен)
            LOGGER.error(e.getMessage());
            throw new NonRetryableException(e);
        }


    }
}
