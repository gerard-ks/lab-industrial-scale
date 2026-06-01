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

    @Transactional // Création de la commande en statut PENDING
    public void createPendingOrder(UUID orderId, String itemName, double amount) {
        jdbcClient.sql("INSERT INTO order_schema.orders (id, item_name, amount, status) VALUES (?, ?, ?, 'PENDING')")
                .param(orderId).param(itemName).param(amount).update();
    }

    @Transactional // Validation finale + Écriture Outbox
    public void validateOrder(UUID orderId, UUID correlationId) {
        jdbcClient.sql("UPDATE order_schema.orders SET status = 'VALIDATED' WHERE id = ?")
                .param(orderId).update();

        OrderValidatedEvent event = new OrderValidatedEvent(UUID.randomUUID(), orderId, 1, correlationId, null);
        publishToOutbox(event, "OrderValidatedEvent", correlationId);
    }

    @Transactional // Compensation + Écriture Outbox
    public void cancelOrder(UUID orderId, UUID correlationId, String reason) {
        jdbcClient.sql("UPDATE order_schema.orders SET status = 'CANCELLED' WHERE id = ?")
                .param(orderId).update();

        OrderCancelledEvent event = new OrderCancelledEvent(UUID.randomUUID(), orderId, 1, correlationId, null, reason);
        publishToOutbox(event, "OrderCancelledEvent", correlationId);
    }

    private void publishToOutbox(Object event, String eventType, UUID correlationId) {
        try {
            jdbcClient.sql("INSERT INTO order_schema.outbox (id, event_type, aggregate_type, aggregate_id, version, correlation_id, causation_id, payload) " +
                            "VALUES (?, ?, 'Order', ?, 1, ?, null, ?::jsonb)")
                    .param(UUID.randomUUID()).param(eventType).param(correlationId).param(correlationId)
                    .param(objectMapper.writeValueAsString(event)).update();
        } catch (Exception e) {
            throw new RuntimeException("Erreur Outbox", e);
        }
    }
}
