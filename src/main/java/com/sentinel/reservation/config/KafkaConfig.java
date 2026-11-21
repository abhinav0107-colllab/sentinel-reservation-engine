package com.sentinel.reservation.config;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.ExponentialBackOff;

@Configuration
public class KafkaConfig {

    @Bean
    NewTopic reservationEvents(SentinelProperties props) {
        return TopicBuilder.name(props.reservationTopic()).partitions(props.topicPartitions()).replicas(1).build();
    }

    @Bean
    NewTopic reservationEventsDlt(SentinelProperties props) {
        return TopicBuilder.name(props.reservationTopic() + ".DLT").partitions(props.topicPartitions()).replicas(1).build();
    }

    /**
     * Payment timeouts are retried with exponential backoff (~10 s total), then dead-lettered. A
     * dead-lettered payment is not lost: the PaymentReconciler resolves UNKNOWN payments by asking
     * the provider, and the hold-expiry sweeper releases the inventory.
     */
    @Bean
    DefaultErrorHandler kafkaErrorHandler(KafkaTemplate<String, String> template) {
        var backoff = new ExponentialBackOff(250, 2.0);
        backoff.setMaxElapsedTime(10_000);
        var handler = new DefaultErrorHandler(new DeadLetterPublishingRecoverer(template), backoff);
        handler.addNotRetryableExceptions(JsonProcessingException.class, IllegalArgumentException.class);
        return handler;
    }
}
