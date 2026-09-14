package com.mktplace.commons.web;

import org.springframework.http.HttpStatus;

import java.util.List;

/**
 * Base das exceções de API: vira Problem Details (RFC 9457) com "type" derivado do código de negócio.
 */
public class ApiException extends RuntimeException {

    public record FieldError(String field, String message) {
    }

    private final HttpStatus status;
    private final String code;
    private final String title;
    private final List<FieldError> errors;

    public ApiException(HttpStatus status, String code, String title, String detail, List<FieldError> errors) {
        super(detail);
        this.status = status;
        this.code = code;
        this.title = title;
        this.errors = List.copyOf(errors);
    }

    public ApiException(HttpStatus status, String code, String title, String detail) {
        this(status, code, title, detail, List.of());
    }

    public HttpStatus status() {
        return status;
    }

    public String code() {
        return code;
    }

    public String title() {
        return title;
    }

    public List<FieldError> errors() {
        return errors;
    }
}
