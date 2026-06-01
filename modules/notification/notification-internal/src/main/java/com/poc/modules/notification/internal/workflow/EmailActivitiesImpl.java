package com.poc.modules.notification.internal.workflow;

import io.temporal.spring.boot.ActivityImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Component
@ActivityImpl(taskQueues = "NotificationTaskQueue")
public class EmailActivitiesImpl implements EmailActivities {
    private static final Logger log = LoggerFactory.getLogger(EmailActivitiesImpl.class);

    @Override
    public void sendEmail(UUID orderId, String message) {
        log.info("[Temporal] Démarrage SMTP pour la commande : {}", orderId);

        // Simulation d'un crash réseau aléatoire
        if (Math.random() > 0.8) {
            log.error("[Temporal] Crash SMTP ! Temporal va réessayer.");
            throw new RuntimeException("SMTP_ERROR");
        }

        log.info("[Temporal] EMAIL ENVOYÉ ! (Message: '{}')", message);
    }
}
