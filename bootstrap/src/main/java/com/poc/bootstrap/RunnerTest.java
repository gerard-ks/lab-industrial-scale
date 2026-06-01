package com.poc.bootstrap;

import com.poc.modules.order.contract.OrderFacade;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Component
public class RunnerTest implements CommandLineRunner {
    private static final Logger log = LoggerFactory.getLogger(RunnerTest.class);

    private final OrderFacade orderFacade;

    // Le Feature Flag pour forcer une rupture de stock
    @Value("${chaos.monkey.force-stock-fail:false}")
    private boolean forceStockFail;

    public RunnerTest(OrderFacade orderFacade) {
        this.orderFacade = orderFacade;
    }

    @Override
    public void run(String... args) throws Exception {
        log.info("==========================================================");
        log.info("DÉMARRAGE DU TEST LAB 3 (TEMPORAL) ...");
        log.info("==========================================================");

        try {
            // Laisse le temps au cluster Docker (Temporal/Redpanda) de s'initialiser
            Thread.sleep(8000);

            UUID orderId = UUID.randomUUID();
            double amount = 2500.00;
            String productId;

            // La logique du Chaos Monkey
            if (forceStockFail) {
                log.warn("CHAOS MONKEY ACTIF : On commande un produit sans stock pour forcer un Rollback !");
                productId = "PRODUIT_EPUISE"; // Nom factice qui fera planter la requête UPDATE de l'Inventory
            } else {
                log.info("[User] Clique sur 'Acheter un PC Gamer'...");
                productId = "PC_GAMER"; // Nom valide qui a une ligne dans inventory_schema.stock
            }

            // L'appel métier, totalement agnostique de Temporal
            orderFacade.placeOrder(orderId, productId, amount);

            log.info("[API] L'intention d'achat est envoyée au module Order !");

        } catch (InterruptedException e) {
            log.error("[Test] Thread interrompu", e);
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            log.error("[Test] Erreur inattendue", e);
        }
    }
}