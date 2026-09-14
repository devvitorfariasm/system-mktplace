package com.mktplace;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Aplicação única. Cada serviço é um profile (order, payment, inventory, notification, audit) e sobe
 * como processo independente com seu próprio banco e consumer group; o pacote "commons" é compartilhado.
 */
@SpringBootApplication
public class MktplaceApplication {

    public static void main(String[] args) {
        SpringApplication.run(MktplaceApplication.class, args);
    }
}
