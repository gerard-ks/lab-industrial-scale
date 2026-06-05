package com.poc.modules.notification.internal;

import com.poc.modules.order.contract.events.OrderCancelledEvent;
import com.poc.modules.order.contract.events.OrderValidatedEvent;
import com.poc.modules.notification.internal.workflow.EmailWorkflow;
import com.poc.platform.messaging.InboxProcessor;
import io.temporal.api.enums.v1.WorkflowIdConflictPolicy;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowOptions;
import io.temporal.failure.TemporalException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import java.time.Duration;
import java.util.UUID;

@Component
public class NotificationInboxWorker {

    private static final Logger log = LoggerFactory.getLogger(NotificationInboxWorker.class);
    private final InboxProcessor inboxProcessor;
    private final WorkflowClient temporalClient;

    public NotificationInboxWorker(InboxProcessor inboxProcessor, WorkflowClient temporalClient) {
        this.inboxProcessor = inboxProcessor;
        this.temporalClient = temporalClient;
    }

    @Scheduled(fixedDelay = 2000)
    public void consumeNotifications() {
        // Traitement étanche des succès
        inboxProcessor.processInbox(
                "notification_schema",
                "Notification-OrderValidatedEvent",
                OrderValidatedEvent.class,
                event -> enqueueEmailTask(event.aggregateId(), "Votre commande est confirmée !", "VALIDATED")
        );

        // Traitement étanche des échecs (Saga Rollback)
        inboxProcessor.processInbox(
                "notification_schema",
                "Notification-OrderCancelledEvent",
                OrderCancelledEvent.class,
                event -> enqueueEmailTask(event.aggregateId(), "Commande annulée. Raison : " + event.reason(), "CANCELLED")
        );
    }

    private void enqueueEmailTask(UUID orderId, String message, String actionType) {
        // BONNE PRATIQUE : Le WorkflowId intègre le type d'action pour différencier le mail de succès du mail d'échec
        WorkflowOptions options = WorkflowOptions.newBuilder()
                .setTaskQueue("NotificationTaskQueue")
                .setWorkflowId("SendEmail-" + actionType + "-" + orderId)

                // VERROU ANTI-SPAM : Si le workflow d'e-mail pour cette commande a déjà été lancé ou terminé,
                // on refuse de le relancer. Temporal lève une exception, évitant le double envoi.
                .setWorkflowIdConflictPolicy(WorkflowIdConflictPolicy.WORKFLOW_ID_CONFLICT_POLICY_FAIL)

                // TIME-BOXING : Un envoi de mail asynchrone ne doit pas rester suspendu des semaines en mémoire
                .setWorkflowRunTimeout(Duration.ofMinutes(10))
                .build();

        EmailWorkflow workflow = temporalClient.newWorkflowStub(EmailWorkflow.class, options);

        try {
            log.info("[Notification-Worker] Demande d'envoi d'email pour la commande {} (Action: {})", orderId, actionType);
            WorkflowClient.start(workflow::sendEmailAsync, orderId, message);

        } catch (TemporalException e) {
            log.error("[Notification-Worker] Erreur infrastructure Temporal pour la commande {}. Rejeu géré par l'Inbox.", orderId, e.getMessage());

            // On re-throw l'erreur pour forcer l'InboxProcessor à faire un Rollback de la transaction
            // et replacer le message en PENDING pour le prochain cycle de 2 secondes.
            throw e;
        }
    }
}
