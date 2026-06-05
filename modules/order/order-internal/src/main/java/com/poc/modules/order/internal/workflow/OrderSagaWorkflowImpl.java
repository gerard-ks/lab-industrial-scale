package com.poc.modules.order.internal.workflow;

import com.poc.modules.inventory.contract.workflow.InventoryActivities;
import com.poc.modules.order.contract.workflow.OrderSagaWorkflow;
import io.temporal.activity.ActivityOptions;
import io.temporal.activity.LocalActivityOptions;
import io.temporal.common.RetryOptions;
import io.temporal.failure.ActivityFailure;
import io.temporal.spring.boot.WorkflowImpl;
import io.temporal.workflow.Workflow;
import org.slf4j.Logger;

import java.time.Duration;
import java.util.UUID;

@WorkflowImpl(taskQueues = "OrderTaskQueue")
public class OrderSagaWorkflowImpl implements OrderSagaWorkflow {

    private final Logger log = Workflow.getLogger(OrderSagaWorkflowImpl.class);

    // Configuration de l'appel local
    private final OrderActivities orderActivities = Workflow.newLocalActivityStub(
            OrderActivities.class,
            LocalActivityOptions.newBuilder()
                    .setStartToCloseTimeout(Duration.ofSeconds(10)) // Local = rapide
                    .setRetryOptions(RetryOptions.newBuilder().setMaximumAttempts(3).build())
                    .build()
    );

    // Configuration de l'appel au Participant (Inventory)
    private final InventoryActivities inventory = Workflow.newActivityStub(
            InventoryActivities.class,
            ActivityOptions.newBuilder()
                    .setTaskQueue("InventoryTaskQueue")
                    .setStartToCloseTimeout(Duration.ofMinutes(1)) // Le fameux Timeout de la Saga !
                    .setRetryOptions(RetryOptions.newBuilder()
                            .setMaximumAttempts(1) // Pour le Lab : pas de retry automatique, on veut voir l'échec
                            .build())
                    .build()
    );

    @Override
    public void executeOrderSaga(OrderRequest request) {
        log.info("[Saga-Temporal] Démarrage du Workflow pour la commande {}", request.orderId());

        // BONNE PRATIQUE : Identifiant déterministe hérité directement de la requête
        UUID correlationId = request.orderId();

        // Drapeau d'état pour suivre la progression de la Saga
        boolean isOrderCreated = false;

        try {
            // ÉTAPE 0 : Création locale
            orderActivities.createPendingOrder(request.orderId(), request.productId(), request.amount());
            isOrderCreated = true; // La commande existe, elle devra être compensée en cas d'échec ultérieur

            // ÉTAPE 1 : Appel à l'Inventaire (Distant)
            inventory.reserveStock(request.orderId(), request.productId());
            log.info("[Saga-Temporal] Stock réservé. Validation de la commande.");

            // ÉTAPE 2 (Happy Path) : Validation locale
            orderActivities.validateOrder(request.orderId(), correlationId);

        } catch (ActivityFailure e) {
            log.error("[Saga-Temporal] Échec de la Saga. Analyse de la compensation nécessaire.", e);

            // BONNE PRATIQUE : On ne compense que si la commande a effectivement été créée
            if (isOrderCreated) {
                log.info("[Saga-Temporal] Déclenchement de la Compensation (Rollback).");
                orderActivities.cancelOrder(request.orderId(), correlationId, "Rupture de Stock ou Échec Système");
            } else {
                log.warn("[Saga-Temporal] Échec avant création de la commande. Aucune compensation requise.");
            }

            throw e;
        }
    }
}