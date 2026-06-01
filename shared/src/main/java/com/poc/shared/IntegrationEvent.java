package com.poc.shared;

import java.util.UUID;

public interface IntegrationEvent {
    UUID eventId();
    String eventType();
    UUID aggregateId();
    String aggregateType();
    int version();
    UUID correlationId();
    UUID causationId();
}
