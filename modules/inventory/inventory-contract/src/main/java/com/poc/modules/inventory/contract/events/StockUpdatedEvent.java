package com.poc.modules.inventory.contract.events;

import com.poc.shared.IntegrationEvent;

import java.util.UUID;

// C'est le JSON qui ira dans l'Outbox puis sur Redpanda
public record StockUpdatedEvent(
        UUID eventId, UUID aggregateId, int version, UUID correlationId, UUID causationId,
        String productId, int newQuantity
) implements IntegrationEvent {
    @Override public String eventType() { return "StockUpdatedEvent"; }
    @Override public String aggregateType() { return "Inventory"; }
}
