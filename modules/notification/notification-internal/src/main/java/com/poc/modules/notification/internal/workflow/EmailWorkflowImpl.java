package com.poc.modules.notification.internal.workflow;

import io.temporal.activity.ActivityOptions;
import io.temporal.common.RetryOptions;
import io.temporal.spring.boot.WorkflowImpl;
import io.temporal.workflow.Workflow;

import java.time.Duration;
import java.util.UUID;

@WorkflowImpl(taskQueues = "NotificationTaskQueue")
public class EmailWorkflowImpl implements EmailWorkflow {

    // On configure l'Activity avec des Retries infinis si le SMTP plante !
    private final EmailActivities emailActivities = Workflow.newActivityStub(
            EmailActivities.class,
            ActivityOptions.newBuilder()
                    .setStartToCloseTimeout(Duration.ofSeconds(10))
                    .setRetryOptions(RetryOptions.newBuilder().setMaximumAttempts(10).build())
                    .build()
    );

    @Override
    public void sendEmailAsync(UUID orderId, String message) {
        // Appel de l'Activity. Temporal bloque ici (en sécurité) si ça plante.
        emailActivities.sendEmail(orderId, message);
    }
}
