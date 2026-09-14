package com.mktplace.commons;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.EnvironmentPostProcessor;
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.io.ClassPathResource;

import java.io.IOException;
import java.io.UncheckedIOException;

/**
 * Carrega META-INF/kafka-commons-defaults.yml com a menor precedência possível:
 * o application.yml de cada serviço e variáveis de ambiente sempre vencem.
 */
public class CommonsDefaultsEnvironmentPostProcessor implements EnvironmentPostProcessor {

    @Override
    public void postProcessEnvironment(ConfigurableEnvironment environment, SpringApplication application) {
        var resource = new ClassPathResource("META-INF/kafka-commons-defaults.yml");
        try {
            new YamlPropertySourceLoader().load("kafka-commons-defaults", resource)
                    .forEach(source -> environment.getPropertySources().addLast(source));
        } catch (IOException e) {
            throw new UncheckedIOException("Não foi possível ler kafka-commons-defaults.yml", e);
        }
    }
}
