package com.ask.nrelate.rt.utils;

/**
 * User: kaniyarasu
 * Date: 11/11/13
 * Time: 4:19 PM
 * Enum for the EventType.
 */
public enum EventType {
    Impression("impression"),
    AdImpression("adImpression"),
    InternalImpression("internalImpression"),
    ExternalImpression("externalImpression"),
    AdClick("adClick"),
    InternalClick("internalClick"),
    ExternalClick("externalClick");

    private String fieldName;

    private EventType(String fieldName) {
        this.fieldName = fieldName;
    }

    public String fieldName() {
        return fieldName;
    }

}
