package com.mktplace.commons.web;

import org.springframework.http.HttpStatus;

public class NotFoundException extends ApiException {

    public NotFoundException(String code, String detail) {
        super(HttpStatus.NOT_FOUND, code, "Recurso não encontrado", detail);
    }
}
