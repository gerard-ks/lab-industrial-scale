package com.poc.modules.notification.internal.workflow;

import io.temporal.activity.ActivityOptions;
import io.temporal.common.RetryOptions;
import io.temporal.spring.boot.WorkflowImpl;
import io.temporal.workflow.Workflow;
import java.time.Duration;
import java.util.UUID;

@WorkflowImpl(taskQueues = "NotificationTaskQueue")
public class EmailWorkflowImpl implements EmailWorkflow {

    // Configuration optimisée pour la résilience réseau réelle
    private final EmailActivities emailActivities = Workflow.newActivityStub(
            EmailActivities.class,
            ActivityOptions.newBuilder()
                    // 1. BONNE PRATIQUE : Hausse du timeout à 30s.
                    // On laisse le temps au protocole TCP/SMTP de gérer sa propre latence ou ses timeouts internes.
                    .setStartToCloseTimeout(Duration.ofSeconds(30))

                    .setRetryOptions(RetryOptions.newBuilder()
                            .setMaximumAttempts(10) // Maintien de vos 10 tentatives de sécurité

                            // 2. BONNE PRATIQUE : Ajout d'un intervalle initial et d'un backoff.
                            // Sans cela, Temporal réessaye instantanément (0s d'attente). Bombardier le serveur SMTP
                            // à la milliseconde près en cas d'erreur réseau aggrave la panne de l'infrastructure.
                            .setInitialInterval(Duration.ofSeconds(3))
                            .setBackoffCoefficient(2.0) // Espace les essais (3s, 6s, 12s, 24s...) pour laisser le réseau respirer
                            .build())
                    .build()
    );

    @Override
    public void sendEmailAsync(UUID orderId, String message) {
        // Appel sécurisé. Temporal gère le cycle des 10 tentatives avec backoff de manière transparente.
        emailActivities.sendEmail(orderId, message);
    }
}
