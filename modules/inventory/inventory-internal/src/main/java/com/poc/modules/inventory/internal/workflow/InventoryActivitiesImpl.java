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
    private final ObjectMapper objectMapper;

    public InventoryActivitiesImpl(JdbcClient jdbcClient, ObjectMapper objectMapper) {
        this.jdbcClient = jdbcClient;
        this.objectMapper = objectMapper;
    }

    @Override
    @Transactional // Englobe le Stock et l'Outbox
    public void reserveStock(UUID orderId, String productId) {
        log.info("[Inventory-Temporal] Demande de réservation pour la commande {} et le produit {}", orderId, productId);

        // 1. ID d'événement déterministe : si Temporal rejoue l'activité pour la même commande,
        // nameUUIDFromBytes produira EXACTEMENT le même UUID.
        UUID eventId = UUID.nameUUIDFromBytes(("stock-reservation-" + orderId.toString()).getBytes());

        // 2. PROTECTION ANTI-REJEU : On vérifie si l'événement n'existe pas déjà dans l'Outbox
        boolean alreadyProcessed = jdbcClient.sql("SELECT COUNT(*) FROM inventory_schema.outbox WHERE id = ?")
                .param(eventId)
                .query(Integer.class)
                .single() > 0;

        if (alreadyProcessed) {
            log.warn("[Inventory-Temporal] Détection d'un rejeu Temporal pour la commande {}. Action ignorée (Idempotence).", orderId);
            return; // Le stock a déjà été déduit au cycle précédent, on valide le succès sans re-déduire
        }

        // 3. Déduction du stock avec verrou pessimiste
        int rowsUpdated = jdbcClient.sql(
                        "UPDATE inventory_schema.stock SET quantity = quantity - 1 WHERE product_id = ? AND quantity >= 1")
                .param(productId)
                .update();

        if (rowsUpdated > 0) {
            log.info("[Inventory-Temporal] Stock déduit avec succès !");
            publishStockUpdate(eventId, productId, orderId);
        } else {
            log.warn("[Inventory-Temporal] RUPTURE DE STOCK ! Rejet de la demande.");
            throw new RuntimeException("OUT_OF_STOCK");
        }
    }

    private void publishStockUpdate(UUID eventId, String productId, UUID correlationId) {
        try {
            int newQuantity = 0;
            UUID aggregateId = UUID.nameUUIDFromBytes(productId.getBytes());

            // On injecte l'eventId déterministe calculé en amont
            StockUpdatedEvent event = new StockUpdatedEvent(
                    eventId,
                    aggregateId,
                    1,
                    correlationId,
                    null,
                    productId,
                    newQuantity
            );

            jdbcClient.sql(
                            "INSERT INTO inventory_schema.outbox " +
                                    "(id, event_type, aggregate_type, aggregate_id, version, correlation_id, causation_id, payload) " +
                                    "VALUES (?, ?, 'Inventory', ?, 1, ?, ?, ?::jsonb) " +
                                    "ON CONFLICT (id) DO NOTHING" // Sécurité supplémentaire au niveau de la contrainte PK
                    )
                    .param(event.eventId())
                    .param(event.eventType())
                    .param(event.aggregateId())
                    .param(event.correlationId())
                    .param(event.causationId())
                    .param(objectMapper.writeValueAsString(event))
                    .update();

        } catch (Exception e) {
            throw new RuntimeException("Erreur de publication Outbox", e);
        }
    }
}
