package com.eai.person.model;

import java.time.Instant;
import java.time.LocalDate;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/** Canonical person entity mapped to table PERSON. */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Person {
    private Long id;
    private String firstName;
    private String lastName;
    private String email;
    private LocalDate birthDate;
    private String country;
    // enriched fields
    private String fullName;
    private Integer age;
    private String source;
    private Instant createdAt;
}
