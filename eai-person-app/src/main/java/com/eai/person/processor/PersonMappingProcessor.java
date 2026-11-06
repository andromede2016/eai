package com.eai.person.processor;

import java.time.LocalDate;
import java.util.Map;

import com.eai.framework.core.Exchange;
import com.eai.framework.spi.Processor;
import com.eai.person.model.Person;

/** Maps decoded XML values (Map) to the Person entity. */
public class PersonMappingProcessor implements Processor {

    @Override
    public void process(Exchange ex) {
        Map<?, ?> m = (Map<?, ?>) ex.getMessage().getBody();
        String email = get(m, "email");
        String bd = get(m, "birthDate");
        ex.getMessage().setBody(Person.builder()
            .id(Long.valueOf(get(m, "id")))
            .firstName(get(m, "firstName"))
            .lastName(get(m, "lastName"))
            .email(email == null ? null : email.toLowerCase())
            .birthDate(bd == null ? null : LocalDate.parse(bd))
            .country(get(m, "country"))
            .build());
    }

    private static String get(Map<?, ?> m, String k) {
        Object v = m.get(k);
        return v == null || v.toString().isBlank() ? null : v.toString().trim();
    }
}
