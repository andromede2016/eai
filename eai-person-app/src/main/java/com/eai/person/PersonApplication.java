package com.eai.person;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

/** Server entry point: JMS (XML) -> validate -> map -> enrich -> insert into PERSON. */
@SpringBootApplication
@ConfigurationPropertiesScan
public class PersonApplication {

    public static void main(String[] args) {
        SpringApplication.run(PersonApplication.class, args);
    }
}
