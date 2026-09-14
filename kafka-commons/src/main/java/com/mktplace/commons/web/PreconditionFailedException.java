package com.mktplace.commons.web;

import org.springframework.http.HttpStatus;

/** 412: If-Match divergente da versão atual do recurso. */
public class PreconditionFailedException extends ApiException {

    public PreconditionFailedException(String detail) {
        super(HttpStatus.PRECONDITION_FAILED, "version-mismatch", "Versão desatualizada", detail);
    }
}
