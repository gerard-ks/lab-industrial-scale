package com.poc.modules.order.contract.events;

import com.poc.shared.IntegrationEvent;

import java.util.UUID;

public record OrderCancelledEvent(
        UUID eventId,
        UUID aggregateId,
        int version,
        UUID correlationId,
        UUID causationId,
        String reason
) implements IntegrationEvent {
    @Override
    public String eventType() { return "OrderCancelledEvent"; }

    @Override
    public String aggregateType() { return "Order"; }
}
