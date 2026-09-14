package com.mktplace.commons.web;

import org.springframework.http.HttpStatus;

import java.util.List;

/** 422: regra de negócio violada (item duplicado, quantidade ≤ 0, placeholder desconhecido...). */
public class BusinessException extends ApiException {

    public BusinessException(String code, String title, String detail) {
        super(HttpStatus.UNPROCESSABLE_CONTENT, code, title, detail);
    }

    public BusinessException(String code, String title, String detail, List<FieldError> errors) {
        super(HttpStatus.UNPROCESSABLE_CONTENT, code, title, detail, errors);
    }
}
