package com.poc.modules.order.contract.workflow;

import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;

import java.util.UUID;

@WorkflowInterface
public interface OrderSagaWorkflow {

    // La méthode d'entrée de la Saga.
    // L'annotation indique à Temporal que c'est un point de démarrage.
    @WorkflowMethod
    void executeOrderSaga(OrderRequest request);

    // Un DTO simple pour passer les arguments au Workflow
    record OrderRequest(UUID orderId, String productId, double amount) {}
}
