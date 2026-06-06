package com.poc.modules.order.internal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
public class OrderRetentionCleaner {
    private static final Logger log = LoggerFactory.getLogger(OrderRetentionCleaner.class);
    private final JdbcClient jdbcClient;

    public OrderRetentionCleaner(JdbcClient jdbcClient) {
        this.jdbcClient = jdbcClient;
    }

    @Scheduled(cron = "0 0 2 * * ?") // S'exécute toutes les nuits à 2h du matin
    @Transactional
    public void purgeOrderOutbox() {
        log.info("[Order-Module] Nettoyage de la table Outbox...");
        int rows = jdbcClient.sql(
                "DELETE FROM order_schema.outbox WHERE created_at < NOW() - INTERVAL '1 day'"
        ).update();
        log.info("[Order-Module] Outbox purgée. Lignes supprimées : {}", rows);
    }
}
