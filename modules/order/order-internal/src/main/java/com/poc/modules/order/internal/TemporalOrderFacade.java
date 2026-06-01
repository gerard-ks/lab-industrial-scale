package com.poc.modules.order.internal;

import com.poc.modules.order.contract.OrderFacade;
import com.poc.modules.order.contract.workflow.OrderSagaWorkflow;
import com.poc.modules.order.contract.workflow.OrderSagaWorkflow.OrderRequest;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowOptions;
import org.springframework.stereotype.Service;

import java.util.UUID;

@Service
public class TemporalOrderFacade implements OrderFacade {

    private final WorkflowClient temporalClient;

    public TemporalOrderFacade(WorkflowClient temporalClient) {
        this.temporalClient = temporalClient;
    }

    @Override
    public void placeOrder(UUID orderId, String productId, double amount) {
        // La logique d'appel Temporal est cachée ici !
        WorkflowOptions options = WorkflowOptions.newBuilder()
                .setTaskQueue("OrderTaskQueue")
                .setWorkflowId("OrderSaga-" + orderId)
                .build();

        OrderSagaWorkflow workflow = temporalClient.newWorkflowStub(OrderSagaWorkflow.class, options);
        OrderRequest request = new OrderRequest(orderId, productId, amount);

        // Démarrage asynchrone
        WorkflowClient.start(workflow::executeOrderSaga, request);
    }
}
