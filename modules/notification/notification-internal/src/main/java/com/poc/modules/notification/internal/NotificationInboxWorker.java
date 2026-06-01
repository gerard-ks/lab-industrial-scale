package com.poc.modules.notification.internal;

import com.poc.modules.order.contract.events.OrderCancelledEvent;
import com.poc.modules.order.contract.events.OrderValidatedEvent;
import com.poc.modules.notification.internal.workflow.EmailWorkflow;
import com.poc.platform.messaging.InboxProcessor;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowOptions;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Component
public class NotificationInboxWorker {

    private final InboxProcessor inboxProcessor;
    private final WorkflowClient temporalClient; // Le point d'entrée vers Temporal Server

    public NotificationInboxWorker(InboxProcessor inboxProcessor, WorkflowClient temporalClient) {
        this.inboxProcessor = inboxProcessor;
        this.temporalClient = temporalClient;
    }

    @Scheduled(fixedDelay = 2000)
    public void consumeNotifications() {
        // Succès
        inboxProcessor.processInbox("notification_schema", "NotificationConsumer", OrderValidatedEvent.class,
                event -> enqueueEmailTask(event.aggregateId(), "Votre commande est confirmée !"));

        // Échecs
        inboxProcessor.processInbox("notification_schema", "NotificationConsumer", OrderCancelledEvent.class,
                event -> enqueueEmailTask(event.aggregateId(), "Commande annulée. Raison : " + event.reason()));
    }

    // Le Fire-And-Forget vers Temporal
    private void enqueueEmailTask(UUID orderId, String message) {
        WorkflowOptions options = WorkflowOptions.newBuilder()
                .setTaskQueue("NotificationTaskQueue")
                .setWorkflowId("SendEmail-" + orderId)
                .build();

        EmailWorkflow workflow = temporalClient.newWorkflowStub(EmailWorkflow.class, options);
        WorkflowClient.start(workflow::sendEmailAsync, orderId, message);
    }
}