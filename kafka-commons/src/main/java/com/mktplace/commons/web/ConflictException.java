package com.mktplace.commons.web;

import org.springframework.http.HttpStatus;

/** 409: estado atual não permite a operação (transição inválida, email já existe, grupo ativo). */
public class ConflictException extends ApiException {

    public ConflictException(String code, String detail) {
        super(HttpStatus.CONFLICT, code, "Conflito com o estado atual", detail);
    }
}
