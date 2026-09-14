package com.mktplace.commons.chaos;

/** TRANSIENT passa pelo retry com backoff; POISON vai direto para a DLT. */
public enum ChaosMode {
    TRANSIENT, POISON
}
