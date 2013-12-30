package com.ask.nrelate.rt.utils;

/**
 * Created by IntelliJ IDEA.
 * User: kaniyarasu
 * Date: 6/11/13
 * Time: 5:55 PM
 * To change this template use File | Settings | File Templates.
 */
public enum RTLogType {
    IMPRESSION("impression"),
    INT("internal"),
    AD("ad"),
    EXT("external");

    private String fieldName;

    private RTLogType(String fieldName) {
        this.fieldName = fieldName;
    }

    public String fieldName() {
        return fieldName;
    }
}
