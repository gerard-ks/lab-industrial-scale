package com.poc.modules.notification.internal;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.poc.modules.notification.internal.workflow.EmailWorkflow;
import io.nats.client.*;
import io.nats.client.api.ConsumerConfiguration;
import io.temporal.api.enums.v1.WorkflowIdConflictPolicy;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowOptions;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;
import java.time.Duration;
import java.util.UUID;

@Component
public class NatsToInboxListener {
    private static final Logger log = LoggerFactory.getLogger(NatsToInboxListener.class);

    private final Connection natsConnection;
    private final JdbcClient jdbcClient;
    private final TransactionTemplate txTemplate;
    private final ObjectMapper objectMapper;
    private final WorkflowClient temporalClient;
    private JetStreamSubscription subscription;

    public NatsToInboxListener(Connection natsConnection, JdbcClient jdbcClient,
                               TransactionTemplate txTemplate, ObjectMapper objectMapper,
                               WorkflowClient temporalClient) {
        this.natsConnection = natsConnection;
        this.jdbcClient = jdbcClient;
        this.txTemplate = txTemplate;
        this.objectMapper = objectMapper;
        this.temporalClient = temporalClient;
    }

    @PostConstruct
    public void initSubscription() throws Exception {
        JetStream js = natsConnection.jetStream();

        // 1. CONFIGURATION DE RÉSILIENCE STRICTE SANS INFRA ADDITIONNELLE (MILAN JOVANOVIC 2026)
        ConsumerConfiguration consumerConfig = ConsumerConfiguration.builder()
                .durable("notification-consumer") // S'aligne sur le consommateur persistant NATS UI

                // PROTECTION CONTRE LES MESSAGES POISONS : Remplace l'ancienne colonne 'retry_count'
                // Si le code Java crashe 3 fois en boucle, NATS pousse automatiquement le message dans la DLQ_EVENTS
                .maxDeliver(3)

                // BACKOFF EXPONENTIEL INFRASTRUCTURE : Laisse respirer le système en cas de micro-coupure SMTP/Temporal
                .backoff(Duration.ofSeconds(3), Duration.ofSeconds(6), Duration.ofSeconds(12))
                .build();

        PushSubscribeOptions options = PushSubscribeOptions.builder()
                .configuration(consumerConfig)
                .build();

        // Le Dispatcher orchestre la consommation asynchrone des messages
        Dispatcher dispatcher = natsConnection.createDispatcher((msg) -> {});

        // Écoute du flux CDC étanche généré par Debezium Server
        this.subscription = js.subscribe("events.order_schema.*", dispatcher, new MessageHandler() {
            @Override
            public void onMessage(Message msg) {
                handleMessageTransactionally(msg);
            }
        }, false, options); // autoAck = false pour garder la maîtrise de la transaction

        log.info("[NATS-Milan] Écouteur CDC JetStream configuré en Push-Driven sur 'events.order_schema.*'");
    }

    private void handleMessageTransactionally(Message msg) {
        try {
            // Lecture du JSON enveloppé et aplati (unwrapped) de Debezium
            JsonNode rootNode = objectMapper.readTree(msg.getData());
            JsonNode payloadNode = rootNode.get("payload");

            if (payloadNode == null || payloadNode.isNull()) {
                msg.ack(); // Message technique vide, on l'éjecte
                return;
            }

            UUID messageId = UUID.fromString(payloadNode.get("id").asText());
            UUID correlationId = UUID.fromString(payloadNode.get("correlation_id").asText());
            String eventType = payloadNode.get("event_type").asText();
            String businessPayloadRaw = payloadNode.get("payload").asText();

            // Extraction profonde de l'aggregate_id pour piloter le WorkflowId de Temporal
            JsonNode businessPayload = objectMapper.readTree(businessPayloadRaw);
            UUID orderId = UUID.fromString(businessPayload.get("aggregateId").asText());

            // Traduction dynamique pour le type de mail à envoyer
            String actionType = eventType.equals("OrderValidatedEvent") ? "VALIDATED" : "CANCELLED";
            String emailMessage = actionType.equals("VALIDATED") ? "Votre commande est confirmée !" : "Commande annulée.";
            String dynamicHandlerName = "Notification-" + eventType;

            // 🏆 FUSION TRANSACTIONNELLE (MILAN JOVANOVIC) : Idempotence SQL + Action Temp réelle
            txTemplate.executeWithoutResult(status -> {
                // Étape A : Tentative d'insertion dans le registre de déduplication (Inbox épurée)
                // Si le message est un doublon réseau tardif, la PK bloque et rowsInserted vaudra 0
                int rowsInserted = jdbcClient.sql(
                                "INSERT INTO notification_schema.inbox (message_id, handler_name, correlation_id, payload, processed_at) " +
                                        "VALUES (?, ?, ?, ?::jsonb, NOW()) " +
                                        "ON CONFLICT (message_id, handler_name) DO NOTHING"
                        )
                        .param(messageId)
                        .param(dynamicHandlerName)
                        .param(correlationId)
                        .param(businessPayloadRaw)
                        .update();

                if (rowsInserted == 0) {
                    log.warn("[NATS-Milan] Doublon réseau intercepté par l'Inbox pour le message {}. Traitement annulé.", messageId);
                    return; // Fin de transaction propre, le message sera acquitté et sorti de NATS
                }

                // Étape B : Déclenchement EN TEMPS RÉEL (0 ms de latence artificielle) vers Temporal
                log.info("[NATS-Milan] Message validé de manière idempotente. Envoi immédiat à Temporal pour la commande : {}", orderId);
                enqueueEmailTask(orderId, emailMessage, actionType);
            });

            // Étape C : Tout a réussi (SQL + gRPC Temporal), on valide l'effacement du message dans NATS
            msg.ack();

        } catch (Exception e) {
            // SÉCURITÉ ABSOLUE : Si Temporal ou Postgres crash au milieu du traitement, 
            // la transaction Spring fait un ROLLBACK. L'Inbox reste propre et vide.
            // On ne fait PAS de msg.ack(). NATS JetStream va appliquer son calendrier de backoff (3s, 6s...).
            log.error("[NATS-Milan] Échec critique du traitement transactionnel immédiat. Suspension pour rejeu.", e);
        }
    }

    private void enqueueEmailTask(UUID orderId, String message, String actionType) {
        WorkflowOptions options = WorkflowOptions.newBuilder()
                .setTaskQueue("NotificationTaskQueue")
                .setWorkflowId("SendEmail-" + actionType + "-" + orderId)

                // VERROU ANTI-SPAM INFRASTRUCTURE : Deuxième couche de déduplication par Temporal
                .setWorkflowIdConflictPolicy(WorkflowIdConflictPolicy.WORKFLOW_ID_CONFLICT_POLICY_FAIL)
                .setWorkflowRunTimeout(Duration.ofMinutes(10))
                .build();

        EmailWorkflow workflow = temporalClient.newWorkflowStub(EmailWorkflow.class, options);

        // Démarrage asynchrone (Fire-and-forget) vers le cluster Temporal
        WorkflowClient.start(workflow::sendEmailAsync, orderId, message);
    }
}
