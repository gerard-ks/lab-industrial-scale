package com.poc.platform.messaging;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.regex.Pattern;

@Component
public class InboxProcessor {

    private static final Logger log = LoggerFactory.getLogger(InboxProcessor.class);
    private static final Pattern SCHEMA_PATTERN = Pattern.compile("^[a-zA-Z0-9_]+$");

    private final JdbcClient jdbcClient;
    private final TransactionTemplate txTemplate;
    private final ObjectMapper objectMapper;

    public InboxProcessor(JdbcClient jdbcClient, TransactionTemplate txTemplate, ObjectMapper objectMapper) {
        this.jdbcClient = jdbcClient;
        this.txTemplate = txTemplate;
        this.objectMapper = objectMapper;
    }

    /**
     * Draine l'Inbox locale (remplie par Redpanda) et exécute la logique métier de manière transactionnelle.
     */
    public <T> void processInbox(String schemaName, String handlerName, Class<T> eventClass, Consumer<T> businessLogic) {
        if (schemaName == null || !SCHEMA_PATTERN.matcher(schemaName).matches()) {
            return;
        }

        // On utilise l'astuce CTE (Claim Check) pour verrouiller et passer à PROCESSING instantanément
        String claimSql = String.format(
                "WITH claimed AS ( " +
                        "  SELECT message_id, handler_name FROM %s.inbox " +
                        "  WHERE handler_name = ? AND status = 'PENDING' " +
                        "  ORDER BY received_at ASC LIMIT 100 FOR UPDATE SKIP LOCKED " +
                        ") " +
                        "UPDATE %s.inbox i SET status = 'PROCESSING' " +
                        "FROM claimed c WHERE i.message_id = c.message_id AND i.handler_name = c.handler_name " +
                        "RETURNING i.message_id as messageId, i.payload as payload, i.retry_count as retryCount",
                schemaName, schemaName
        );

        String updateProcessedSql = String.format("UPDATE %s.inbox SET status = 'PROCESSED', processed_at = NOW() WHERE message_id = ? AND handler_name = ?", schemaName);
        String updateFailedSql = String.format("UPDATE %s.inbox SET status = CASE WHEN retry_count + 1 >= 5 THEN 'FAILED' ELSE 'PENDING' END, retry_count = retry_count + 1, last_error = ? WHERE message_id = ? AND handler_name = ?", schemaName);

        boolean hasMoreMessages = true;

        try {
            while (hasMoreMessages) {
                // 1. Transaction A : Le Claim (Rapide)
                List<InboxMessage> messages = txTemplate.execute(status ->
                        jdbcClient.sql(claimSql)
                                .param(handlerName)
                                .query(InboxMessage.class)
                                .list()
                );

                if (messages == null || messages.isEmpty()) {
                    hasMoreMessages = false;
                    continue;
                }

                // 2. Traitement des messages (En dehors du verrou global)
                for (InboxMessage msg : messages) {
                    try {
                        T event = objectMapper.readValue(msg.payload(), eventClass);

                        // 3. Transaction B : Exécution métier + Passage à PROCESSED
                        txTemplate.executeWithoutResult(status -> {
                            businessLogic.accept(event);
                            jdbcClient.sql(updateProcessedSql)
                                    .param(msg.messageId())
                                    .param(handlerName)
                                    .update();
                        });

                        log.debug("[Inbox] Message {} traité avec succès par {}", msg.messageId(), handlerName);

                    } catch (Exception e) {
                        log.error("[Inbox] Échec du traitement métier pour le message {}", msg.messageId(), e);

                        // 4. Échec : Retour à PENDING ou passage en FAILED (DLQ Locale)
                        txTemplate.executeWithoutResult(status ->
                                jdbcClient.sql(updateFailedSql)
                                        .param(e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName())
                                        .param(msg.messageId())
                                        .param(handlerName)
                                        .update()
                        );
                    }
                }
            }
        } catch (Exception e) {
            log.error("[Inbox] Erreur globale lors du traitement de l'Inbox", e);
        }
    }

    // Le DTO interne
    private record InboxMessage(UUID messageId, String payload, int retryCount) {}
}