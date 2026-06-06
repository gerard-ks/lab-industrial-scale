package com.poc.platform.config;

import io.nats.client.Connection;
import io.nats.client.Nats;
import io.nats.client.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import java.time.Duration;

@Configuration
public class NatsConfig {
    private static final Logger log = LoggerFactory.getLogger(NatsConfig.class);

    @Bean(destroyMethod = "close") // Ferme proprement la socket à l'arrêt de Spring
    public Connection natsConnection() throws Exception {
        log.info("[NATS-Config] Connexion au serveur NATS JetStream sur nats://localhost:4222...");

        Options options = new Options.Builder()
                .server("nats://localhost:4222")
                // Reconnexion automatique infinie en cas de coupure de NATS
                .maxReconnects(-1)
                .reconnectWait(Duration.ofSeconds(2))
                .connectionTimeout(Duration.ofSeconds(5))
                .build();

        return Nats.connect(options);
    }
}
