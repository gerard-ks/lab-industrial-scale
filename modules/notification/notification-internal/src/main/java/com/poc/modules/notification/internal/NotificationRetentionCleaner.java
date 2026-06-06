package com.poc.modules.notification.internal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
public class NotificationRetentionCleaner {
    private static final Logger log = LoggerFactory.getLogger(NotificationRetentionCleaner.class);
    private final JdbcClient jdbcClient;

    public NotificationRetentionCleaner(JdbcClient jdbcClient) {
        this.jdbcClient = jdbcClient;
    }

    @Scheduled(cron = "0 30 2 * * ?") // Décalé à 2h30 du matin pour lisser la charge CPU de Postgres
    @Transactional
    public void purgeNotificationInbox() {
        log.info("[Notification-Module] Nettoyage du registre d'idempotence Inbox...");
        int rows = jdbcClient.sql(
                "DELETE FROM notification_schema.inbox WHERE processed_at < NOW() - INTERVAL '7 days'"
        ).update();
        log.info("[Notification-Module] Inbox purgée. Lignes obsolètes supprimées : {}", rows);
    }
}
