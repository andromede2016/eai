package com.eai.framework.spi;

import java.util.List;

import com.eai.framework.core.Exchange;
import com.eai.framework.core.ValidationError;

@FunctionalInterface
public interface Validator {
    /** @return empty list if valid. */
    List<ValidationError> validate(Exchange exchange);
}
