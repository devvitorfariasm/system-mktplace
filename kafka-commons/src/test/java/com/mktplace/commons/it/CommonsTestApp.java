package com.mktplace.commons.it;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/** Aplicação mínima que usa o kafka-commons como qualquer serviço usaria: só depende do módulo. */
@SpringBootApplication
public class CommonsTestApp {

    public static void main(String[] args) {
        SpringApplication.run(CommonsTestApp.class, args);
    }
}
