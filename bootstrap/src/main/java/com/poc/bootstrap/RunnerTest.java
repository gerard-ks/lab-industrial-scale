package com.poc.bootstrap;

import com.poc.modules.order.contract.OrderFacade;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Component
public class RunnerTest {

    private static final Logger log = LoggerFactory.getLogger(RunnerTest.class);

    private final OrderFacade orderFacade;

    @Value("${chaos.monkey.force-stock-fail:false}")
    private boolean forceStockFail;

    public RunnerTest(OrderFacade orderFacade) {
        this.orderFacade = orderFacade;
    }

    @Async
    @EventListener(ApplicationReadyEvent.class)
    public void runTest() {
        try {
            log.info("==========================================================");
            log.info("DÉMARRAGE DU TEST LAB 3 (TEMPORAL) ...");
            log.info("==========================================================");

            // Cette pause ne bloque plus l'initialisation globale de l'application
            Thread.sleep(3000);

            UUID orderId = UUID.randomUUID();
            String productId = forceStockFail ? "PRODUIT_EPUISE" : "PC_GAMER";
            double amount = 2500.00;

            if (forceStockFail) {
                log.warn("CHAOS MONKEY ACTIF : On commande un produit sans stock pour forcer un Rollback !");
            } else {
                log.info("[User] Clique sur 'Acheter un {}'...", productId);
            }

            // L'APPEL MÉTIER PUR
            orderFacade.placeOrder(orderId, productId, amount);

            log.info("[API] L'intention d'achat est envoyée au module Order !");

        } catch (InterruptedException e) {
            log.error("[Test] Le thread de test a été interrompu", e);
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            log.error("[Test] Erreur inattendue", e);
        }
    }
}