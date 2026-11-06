package com.eai.person.enrich;

import java.time.Clock;
import java.time.Instant;
import java.time.LocalDate;
import java.time.Period;

import com.eai.framework.core.Exchange;
import com.eai.framework.core.Headers;
import com.eai.framework.spi.Enricher;
import com.eai.person.model.Person;

import lombok.RequiredArgsConstructor;

/** Adds computed fields: full name, age, source channel, creation timestamp. */
@RequiredArgsConstructor
public class PersonEnricher implements Enricher {

    private final Clock clock;

    public PersonEnricher() { this(Clock.systemDefaultZone()); }

    @Override
    public void enrich(Exchange ex) {
        Person p = ex.getMessage().getBody(Person.class);
        p.setFullName(p.getFirstName() + " " + p.getLastName().toUpperCase());
        if (p.getBirthDate() != null) p.setAge(Period.between(p.getBirthDate(), LocalDate.now(clock)).getYears());
        p.setSource(ex.getMessage().getHeaderAsString(Headers.SOURCE_CHANNEL));
        p.setCreatedAt(Instant.now(clock));
    }
}
