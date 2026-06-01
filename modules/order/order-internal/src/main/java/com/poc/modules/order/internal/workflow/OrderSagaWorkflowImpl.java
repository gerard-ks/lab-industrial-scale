package com.poc.modules.order.internal.workflow;

import com.poc.modules.inventory.contract.workflow.InventoryActivities;
import com.poc.modules.order.contract.workflow.OrderSagaWorkflow;
import io.temporal.activity.ActivityOptions;
import io.temporal.activity.LocalActivityOptions;
import io.temporal.common.RetryOptions;
import io.temporal.spring.boot.WorkflowImpl;
import io.temporal.workflow.Workflow;
import org.slf4j.Logger;

import java.time.Duration;
import java.util.UUID;

// Magie du Spring Boot Starter Temporal (Auto-enregistrement dans la TaskQueue)
@WorkflowImpl(taskQueues = "OrderTaskQueue")
public class OrderSagaWorkflowImpl implements OrderSagaWorkflow {

    private final Logger log = Workflow.getLogger(OrderSagaWorkflowImpl.class);

    private final OrderActivities orderActivities = Workflow.newLocalActivityStub(
            OrderActivities.class,
            LocalActivityOptions.newBuilder()
                    .setStartToCloseTimeout(Duration.ofSeconds(10))
                    .build()
    );

    // 1. Configuration de l'appel au Participant (Inventory)
    private final InventoryActivities inventory = Workflow.newActivityStub(
            InventoryActivities.class,
            ActivityOptions.newBuilder()
                    .setStartToCloseTimeout(Duration.ofMinutes(1)) // Le fameux Timeout de la Saga !
                    .setRetryOptions(RetryOptions.newBuilder()
                            .setMaximumAttempts(1) // Pour le Lab : pas de retry automatique, on veut voir l'échec
                            .build())
                    .build()
    );

    @Override
    public void executeOrderSaga(OrderRequest request) {
        log.info("[Saga-Temporal] Démarrage du Workflow pour la commande {}", request.orderId());

        UUID correlationId = UUID.fromString(Workflow.getInfo().getWorkflowId().replace("OrderSaga-", ""));

        try {

            // C'est maintenant un appel Temporal (Local) pur !
            orderActivities.createPendingOrder(request.orderId(), request.productId(), request.amount());

            // ÉTAPE 1 : Appel à l'Inventaire.
            // Temporal va suspendre ce code Java. Mettre le message dans la DB Temporal.
            // L'envoyer au worker Inventory. Et reprendre ce code quand l'Inventory aura répondu !
            inventory.reserveStock(request.orderId(), request.productId());

            log.info("[Saga-Temporal] Stock réservé. Validation de la commande.");

            // ÉTAPE 2 (Happy Path) : Validation
            orderActivities.validateOrder(request.orderId(), correlationId);

        } catch (Exception e) {
            log.error("[Saga-Temporal] Échec du stock. Déclenchement de la Compensation (Rollback).", e);

            // ÉTAPE 3 (Compensation) : Annulation
            orderActivities.cancelOrder(request.orderId(), correlationId, "Rupture de Stock");

            // On peut re-throw l'erreur pour que le dashboard Temporal affiche la Saga en "FAILED"
            throw Workflow.wrap(e);
        }
    }
}