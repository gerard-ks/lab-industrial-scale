package com.poc.bootstrap;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication(
        scanBasePackages = {
                "com.poc.bootstrap",
                "com.poc.platform",
                "com.poc.modules"
        }
)
@ConfigurationPropertiesScan(
        basePackages = {
                "com.poc.platform",
                "com.poc.modules"
        }
)
@EnableScheduling
public class LabApplication {

    public static void main(String[] args) {
        SpringApplication.run(LabApplication.class, args);
    }
}
