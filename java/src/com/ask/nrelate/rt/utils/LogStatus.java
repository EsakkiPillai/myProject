package com.ask.nrelate.rt.utils;

/**
 * User: kaniyarasu
 * Date: 12/11/13
 * Time: 4:30 PM
 */
public enum LogStatus {

    VALID("Valid"),
    INVALID("Invalid"),
    ERROR("Error"),
    FRAUDULENT("Fraudulent");

    private String fieldName;

    private LogStatus(String fieldName) {
        this.fieldName = fieldName;
    }

    public String fieldName() {
        return fieldName;
    }
}
