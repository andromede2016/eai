package com.eai.person.validation;

import java.time.LocalDate;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import com.eai.framework.core.Exchange;
import com.eai.framework.core.ValidationError;
import com.eai.framework.spi.Validator;
import com.eai.person.codec.XmlCodec;

/** Validates the decoded XML entry before mapping. */
public class PersonXmlValidator implements Validator {

    private static final Pattern EMAIL = Pattern.compile("^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$");
    private static final Pattern COUNTRY = Pattern.compile("^[A-Z]{2}$");

    @Override
    public List<ValidationError> validate(Exchange ex) {
        List<ValidationError> errors = new ArrayList<>();
        if (!(ex.getMessage().getBody() instanceof Map<?, ?> m)) {
            errors.add(new ValidationError("body", "XML payload expected"));
            return errors;
        }
        // root element only exists for XML input (JSON input from Kafka has no root)
        if (m.containsKey(XmlCodec.ROOT) && !"person".equals(m.get(XmlCodec.ROOT))) errors.add(new ValidationError("root", "root element must be <person>"));

        String id = str(m, "id");
        if (id == null) errors.add(new ValidationError("id", "required"));
        else if (!id.matches("\\d{1,18}")) errors.add(new ValidationError("id", "must be numeric"));

        if (str(m, "firstName") == null) errors.add(new ValidationError("firstName", "required"));
        if (str(m, "lastName") == null) errors.add(new ValidationError("lastName", "required"));

        String email = str(m, "email");
        if (email != null && !EMAIL.matcher(email).matches()) errors.add(new ValidationError("email", "invalid format"));

        String country = str(m, "country");
        if (country != null && !COUNTRY.matcher(country).matches()) errors.add(new ValidationError("country", "ISO alpha-2 expected"));

        String bd = str(m, "birthDate");
        if (bd != null) {
            try {
                if (LocalDate.parse(bd).isAfter(LocalDate.now())) errors.add(new ValidationError("birthDate", "in the future"));
            } catch (DateTimeParseException e) {
                errors.add(new ValidationError("birthDate", "yyyy-MM-dd expected"));
            }
        }
        return errors;
    }

    private static String str(Map<?, ?> m, String k) {
        Object v = m.get(k);
        return v == null || v.toString().isBlank() ? null : v.toString().trim();
    }
}
