package com.eai.person.channel;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.regex.Pattern;

import org.springframework.jdbc.core.JdbcTemplate;

import com.eai.framework.core.Exchange;
import com.eai.framework.spi.OutboundChannel;
import com.eai.person.model.Person;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;

/** Target "db": inserts the Person into the table given as destination (default PERSON). */
@RequiredArgsConstructor
public class JdbcPersonOutboundChannel implements OutboundChannel {

    private static final Pattern TABLE = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");

    private final @NonNull JdbcTemplate jdbc;

    @Override public String type() { return "db"; }
    @Override public String defaultFormat() { return "object"; }

    @Override
    public void send(Exchange ex, String destination) {
        String table = destination == null ? "PERSON" : destination;
        if (!TABLE.matcher(table).matches()) throw new IllegalArgumentException("Invalid table name: " + table);
        Person p = ex.getMessage().getBody(Person.class);
        jdbc.update("INSERT INTO " + table
                + " (ID, FIRST_NAME, LAST_NAME, EMAIL, BIRTH_DATE, COUNTRY, FULL_NAME, AGE, SOURCE, CREATED_AT)"
                + " VALUES (?,?,?,?,?,?,?,?,?,?)",
            p.getId(), p.getFirstName(), p.getLastName(), p.getEmail(),
            p.getBirthDate() == null ? null : Date.valueOf(p.getBirthDate()),
            p.getCountry(), p.getFullName(), p.getAge(), p.getSource(),
            Timestamp.from(p.getCreatedAt()));
    }
}
