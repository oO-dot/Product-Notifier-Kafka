package ru.kafkademo.ws.emailnotification.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.util.backoff.FixedBackOff;
import org.springframework.web.client.HttpServerErrorException;
import ru.kafkademo.ws.emailnotification.exception.NonRetryableException;
import ru.kafkademo.ws.emailnotification.exception.RetryableException;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConfig {

    // сетим properties в поле bootstrapServers
    @Value("${spring.kafka.consumer.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.consumer.group-id}")
    private String groupId;

    @Value("${spring.kafka.consumer.properties.spring.json.trusted.packages}")
     private String trustedPackages;

    @Bean
    ConsumerFactory<String, Object> consumerFactory() {
        Map<String, Object> config = new HashMap<>();

        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class); // указываем вместо Deserializer класс wrapper вокруг Deserializer'ов (позволяет скипунть сообщение, которое пришло с Exception(перекидывает на некст оффсет))
        config.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JsonDeserializer.class); // но нам все равно надо указать чем бы будет десериализовывать, поэтому указываем (по сути сделали не на прямую а указали враппер и обернули Deserializer)
        config.put(JsonDeserializer.TRUSTED_PACKAGES, trustedPackages);
        config.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        return new DefaultKafkaConsumerFactory<>(config);
    }

    // здесь идет в фабрику и создает Listener нашего consumer
    // и теперь он будет знать что делать с ошибочными сообщениями
    @Bean
    ConcurrentKafkaListenerContainerFactory<String, Object> kafkaListenerContainerFactory(
            ConsumerFactory<String, Object> consumerFactory, KafkaTemplate kafkaTemplate) {
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(new DeadLetterPublishingRecoverer(kafkaTemplate), new FixedBackOff(3000, 3)); // будет создавать новый топик и класть в него message которые не прошли(Exception где будет) (также добавили интервал попыток и их количество для retryableException(3 секунды интервал и  3 попытки))
        errorHandler.addNotRetryableExceptions(NonRetryableException.class); // можем перечислять все NonRetryableException
        errorHandler.addRetryableExceptions(RetryableException.class); // можем перечислять все RetryableException

        ConcurrentKafkaListenerContainerFactory<String, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.setCommonErrorHandler(errorHandler);

        return factory;
    }

    // здесь она будет слать сообщения ошибочные в DLT топик
    @Bean
    KafkaTemplate<String, Object> kafkaTemplate(ProducerFactory<String, Object> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }

    // сконфигурировали для маленького producer внутри consumer, отдаем ее выше
    @Bean
    ProducerFactory<String, Object> producerFactory() {
        Map<String, Object> config = new HashMap<>();

        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

        return new DefaultKafkaProducerFactory<>(config);
    }
}
