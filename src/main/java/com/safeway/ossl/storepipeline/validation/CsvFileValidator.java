package com.safeway.ossl.storepipeline.validation;

import jakarta.validation.ConstraintValidator;
import jakarta.validation.ConstraintValidatorContext;
import org.springframework.http.MediaType;
import org.springframework.web.multipart.MultipartFile;

import java.util.Set;

public class CsvFileValidator implements ConstraintValidator<ValidCsvFile, MultipartFile> {

    private static final Set<String> CSV_CONTENT_TYPES = Set.of(
            "text/csv", "application/vnd.ms-excel", "application/csv", MediaType.TEXT_PLAIN_VALUE);

    @Override
    public boolean isValid(MultipartFile file, ConstraintValidatorContext context) {
        if (file == null || file.isEmpty()) {
            return fail(context,
                    "Uploaded file is missing or empty. Attach a non-empty CSV file under the 'file' form field.");
        }

        String filename = file.getOriginalFilename();
        String contentType = file.getContentType();
        boolean looksLikeCsv = (filename != null && filename.toLowerCase().endsWith(".csv"))
                || (contentType != null && CSV_CONTENT_TYPES.contains(contentType.toLowerCase()));

        if (!looksLikeCsv) {
            return fail(context, "Uploaded file does not appear to be a CSV file. filename=" + filename
                    + ", contentType=" + contentType);
        }
        return true;
    }

    private static boolean fail(ConstraintValidatorContext context, String message) {
        context.disableDefaultConstraintViolation();
        context.buildConstraintViolationWithTemplate(message.replace("{", "\\{").replace("}", "\\}"))
                .addConstraintViolation();
        return false;
    }
}
