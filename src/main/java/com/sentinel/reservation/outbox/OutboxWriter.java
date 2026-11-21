package com.sentinel.reservation.outbox;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

/**
 * Appends an event to the outbox. {@code Propagation.MANDATORY} turns a whole class of bugs
 * ("someone called this outside the business transaction, so the event and the state change can
 * diverge") into an immediate IllegalTransactionStateException.
 */
@Component
public class OutboxWriter {

    private final OutboxRepository repository;
    private final ObjectMapper objectMapper;

    public OutboxWriter(OutboxRepository repository, ObjectMapper objectMapper) {
        this.repository = repository;
        this.objectMapper = objectMapper;
    }

    @Transactional(propagation = Propagation.MANDATORY)
    public void append(UUID eventId, String aggregateType, UUID aggregateId, String eventType, String topic, Object event) {
        try {
            repository.insert(eventId, aggregateType, aggregateId, eventType, topic, objectMapper.writeValueAsString(event));
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Cannot serialize outbox event " + eventId, e);
        }
    }
}
