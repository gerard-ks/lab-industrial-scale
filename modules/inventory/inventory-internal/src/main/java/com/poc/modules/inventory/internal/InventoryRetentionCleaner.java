package com.poc.modules.inventory.internal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
public class InventoryRetentionCleaner {
    private static final Logger log = LoggerFactory.getLogger(InventoryRetentionCleaner.class);
    private final JdbcClient jdbcClient;

    public InventoryRetentionCleaner(JdbcClient jdbcClient) {
        this.jdbcClient = jdbcClient;
    }

    @Scheduled(cron = "0 0 2 * * ?") // À 2h du matin également
    @Transactional
    public void purgeInventoryOutbox() {
        log.info("[Inventory-Module] Nettoyage de la table Outbox...");
        int rows = jdbcClient.sql(
                "DELETE FROM inventory_schema.outbox WHERE created_at < NOW() - INTERVAL '1 day'"
        ).update();
        log.info("[Inventory-Module] Outbox purgée. Lignes supprimées : {}", rows);
    }
}
