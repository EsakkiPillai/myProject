package com.ask.nrelate.rt.utils.json;

/**
 * Created by IntelliJ IDEA.
 * User: kaniyarasu
 * Date: 22/11/13
 * Time: 11:23 AM
 * To change this template use File | Settings | File Templates.
 */
public enum ValidationType {
    ALL("all"),
    TYPE("type"),
    REQ("REQ");

    private String fieldName;

    private ValidationType(String fieldName) {
        this.fieldName = fieldName;
    }

    public String fieldName() {
        return fieldName;
    }
}
