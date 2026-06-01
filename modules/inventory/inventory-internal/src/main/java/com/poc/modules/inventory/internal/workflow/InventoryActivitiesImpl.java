package com.poc.modules.inventory.internal.workflow;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.poc.modules.inventory.contract.events.StockUpdatedEvent;
import com.poc.modules.inventory.contract.workflow.InventoryActivities;
import io.temporal.spring.boot.ActivityImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

@Component
@ActivityImpl(taskQueues = "InventoryTaskQueue")
public class InventoryActivitiesImpl implements InventoryActivities {

    private static final Logger log = LoggerFactory.getLogger(InventoryActivitiesImpl.class);

    private final JdbcClient jdbcClient;
    private final ObjectMapper objectMapper; // On a besoin de Jackson pour sérialiser le JSON dans l'Outbox

    public InventoryActivitiesImpl(JdbcClient jdbcClient, ObjectMapper objectMapper) {
        this.jdbcClient = jdbcClient;
        this.objectMapper = objectMapper;
    }

    @Override
    @Transactional // La transaction englobe le Stock ET l'Outbox !
    public void reserveStock(UUID orderId, String productId) {

        log.info("[Inventory-Temporal] Demande de réservation pour le produit {}", productId);

        // 1. Déduction du stock avec verrou pessimiste
        int rowsUpdated = jdbcClient.sql(
                        "UPDATE inventory_schema.stock SET quantity = quantity - 1 WHERE product_id = ? AND quantity >= 1")
                .param(productId)
                .update();

        // 2. Traitement du résultat
        if (rowsUpdated > 0) {
            log.info("[Inventory-Temporal] Stock déduit avec succès !");

            // 3. LA CHORÉGRAPHIE : On prévient le monde que le stock a changé !
            publishStockUpdate(productId, orderId); // L'orderId sert de causationId !

        } else {
            log.warn("[Inventory-Temporal] RUPTURE DE STOCK ! Rejet de la demande.");
            // On jette l'exception. Temporal s'occupe de faire échouer la Saga.
            throw new RuntimeException("OUT_OF_STOCK");
        }
    }

    // L'outil d'insertion dans l'Outbox
    private void publishStockUpdate(String productId, UUID causationId) {
        try {
            // (Dans un vrai système, on ferait un SELECT pour avoir la nouvelle quantité restante.
            // Ici on met 0 pour l'exemple).
            int newQuantity = 0;

            UUID aggregateId = UUID.nameUUIDFromBytes(productId.getBytes());

            // On utilise le fameux Record que tu as cité !
            StockUpdatedEvent event = new StockUpdatedEvent(
                    UUID.randomUUID(), aggregateId, 1, UUID.randomUUID(), causationId, productId, newQuantity
            );

            // On l'insère dans l'Outbox, et c'est fini. Redpanda viendra l'aspirer.
            jdbcClient.sql(
                            "INSERT INTO inventory_schema.outbox " +
                                    "(id, event_type, aggregate_type, aggregate_id, version, correlation_id, causation_id, payload) " +
                                    "VALUES (?, ?, 'Inventory', ?, 1, ?, ?, ?::jsonb)")
                    .param(event.eventId())
                    .param(event.eventType())
                    .param(event.aggregateId())
                    .param(event.correlationId())
                    .param(event.causationId())
                    .param(objectMapper.writeValueAsString(event))
                    .update();

        } catch (Exception e) {
            // Si la sérialisation plante, le @Transactional annule la déduction de stock. Sécurité totale.
            throw new RuntimeException("Erreur de publication Outbox", e);
        }
    }
}