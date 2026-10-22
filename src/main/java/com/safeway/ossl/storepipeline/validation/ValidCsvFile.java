package com.safeway.ossl.storepipeline.validation;

import jakarta.validation.Constraint;
import jakarta.validation.Payload;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/** Validates that an uploaded {@code MultipartFile} is present, non-empty and looks like a CSV. */
@Documented
@Constraint(validatedBy = CsvFileValidator.class)
@Target({ElementType.PARAMETER, ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
public @interface ValidCsvFile {

    String message() default "Uploaded file must be a non-empty CSV file.";

    Class<?>[] groups() default {};

    Class<? extends Payload>[] payload() default {};
}
