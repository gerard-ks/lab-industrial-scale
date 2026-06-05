package com.poc.modules.order.internal;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.poc.modules.order.contract.events.OrderCancelledEvent;
import com.poc.modules.order.contract.events.OrderValidatedEvent;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

@Service
public class OrderService {
    private final JdbcClient jdbcClient;
    private final ObjectMapper objectMapper;

    public OrderService(JdbcClient jdbcClient, ObjectMapper objectMapper) {
        this.jdbcClient = jdbcClient;
        this.objectMapper = objectMapper;
    }

    @Transactional
    public void createPendingOrder(UUID orderId, String itemName, double amount) {
        // Idempotence native grâce à la contrainte PRIMARY KEY sur l'id de la commande
        jdbcClient.sql("INSERT INTO order_schema.orders (id, item_name, amount, status) VALUES (?, ?, ?, 'PENDING') ON CONFLICT DO NOTHING")
                .param(orderId).param(itemName).param(amount).update();
    }

    @Transactional
    public void validateOrder(UUID orderId, UUID correlationId) {
        jdbcClient.sql("UPDATE order_schema.orders SET status = 'VALIDATED' WHERE id = ?")
                .param(orderId).update();

        // BONNE PRATIQUE : L'ID de l'événement devient déterministe et lié à l'action.
        // nameUUIDFromBytes garantit que si Temporal rejoue cette méthode, le même UUID est produit.
        UUID eventId = UUID.nameUUIDFromBytes(("validation-" + orderId.toString()).getBytes());

        OrderValidatedEvent event = new OrderValidatedEvent(eventId, orderId, 1, correlationId, null);
        publishToOutbox(eventId, event, "OrderValidatedEvent", orderId, correlationId);
    }

    @Transactional
    public void cancelOrder(UUID orderId, UUID correlationId, String reason) {
        jdbcClient.sql("UPDATE order_schema.orders SET status = 'CANCELLED' WHERE id = ?")
                .param(orderId).update();


        // BONNE PRATIQUE : ID déterministe pour l'annulation également
        UUID eventId = UUID.nameUUIDFromBytes(("cancellation-" + orderId.toString()).getBytes());


        OrderCancelledEvent event = new OrderCancelledEvent(eventId, orderId, 1, correlationId, null, reason);
        publishToOutbox(eventId, event, "OrderCancelledEvent", orderId, correlationId);
    }

    private void publishToOutbox(UUID eventId, Object event, String eventType, UUID aggregateId, UUID correlationId) {
        try {
            // Note: processed prend sa valeur DEFAULT FALSE
            jdbcClient.sql("INSERT INTO order_schema.outbox (id, event_type, aggregate_type, aggregate_id, version, correlation_id, causation_id, payload) " +
                            "VALUES (?, ?, 'Order', ?, 1, ?, null, ?::jsonb)"  +
                            "ON CONFLICT (id) DO NOTHING" // Protège l'Outbox contre le rejeu Temporal
                    )
                    .param(eventId)
                    .param(eventType)
                    .param(aggregateId)
                    .param(correlationId)
                    .param(objectMapper.writeValueAsString(event))
                    .update();
        } catch (Exception e) {
            throw new RuntimeException("Erreur Outbox", e);
        }
    }
}