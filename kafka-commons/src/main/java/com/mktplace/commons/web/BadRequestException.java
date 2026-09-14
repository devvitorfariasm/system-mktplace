package com.mktplace.commons.web;

import org.springframework.http.HttpStatus;

/** 400: requisição malformada (cursor inválido, parâmetro fora do formato). */
public class BadRequestException extends ApiException {

    public BadRequestException(String code, String detail) {
        super(HttpStatus.BAD_REQUEST, code, "Requisição inválida", detail);
    }
}
