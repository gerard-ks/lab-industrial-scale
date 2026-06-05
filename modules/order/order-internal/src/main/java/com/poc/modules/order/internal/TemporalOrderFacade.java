package com.poc.modules.order.internal;

import com.poc.modules.order.contract.OrderFacade;
import com.poc.modules.order.contract.workflow.OrderSagaWorkflow;
import com.poc.modules.order.contract.workflow.OrderSagaWorkflow.OrderRequest;
import io.temporal.api.enums.v1.WorkflowIdConflictPolicy;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowOptions;
import io.temporal.failure.TemporalException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import java.time.Duration;
import java.util.UUID;

@Service
public class TemporalOrderFacade implements OrderFacade {

    private static final Logger log = LoggerFactory.getLogger(TemporalOrderFacade.class);
    private final WorkflowClient temporalClient;

    public TemporalOrderFacade(WorkflowClient temporalClient) {
        this.temporalClient = temporalClient;
    }

    @Override
    public void placeOrder(UUID orderId, String productId, double amount) {
        // 1. Configuration blindée des options du Workflow
        WorkflowOptions options = WorkflowOptions.newBuilder()
                .setTaskQueue("OrderTaskQueue")
                .setWorkflowId("OrderSaga-" + orderId)

                // PROTECTION DOUBLE CLIC : Si l'ID de saga existe déjà (actif ou historique récent),
                // Temporal refuse le démarrage et lève une exception pour éviter la duplication.
                .setWorkflowIdConflictPolicy(WorkflowIdConflictPolicy.WORKFLOW_ID_CONFLICT_POLICY_FAIL)

                // TIME-BOXING : On limite la durée de vie maximale de cette Saga globale.
                // Si le système est bloqué pour une raison X au bout de 5 minutes, Temporal la tue.
                .setWorkflowRunTimeout(Duration.ofMinutes(5))
                .build();

        OrderSagaWorkflow workflow = temporalClient.newWorkflowStub(OrderSagaWorkflow.class, options);
        OrderRequest request = new OrderRequest(orderId, productId, amount);

        try {
            log.info("[Façade-Order] Tentative de démarrage asynchrone de la Saga pour la commande : {}", orderId);

            // Démarrage asynchrone (Fire-and-forget réseau gRPC)
            WorkflowClient.start(workflow::executeOrderSaga, request);

            log.info("[Façade-Order] Saga démarrée avec succès pour la commande : {}", orderId);

        } catch (TemporalException e) {
            // 2. Gestion stricte des pannes de l'infrastructure Temporal (Réseau, Timeout, Conflit ID)
            log.error("[Façade-Order] Échec critique Temporal pour la commande {}. Message : {}", orderId, e.getMessage(), e);

            // On encapsule l'erreur technique pour ne pas polluer l'API REST de l'utilisateur
            throw new RuntimeException("Le service de traitement des commandes est temporairement indisponible. Veuillez réessayer.", e);
        }
    }
}
