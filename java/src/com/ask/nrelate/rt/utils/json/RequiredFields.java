package com.ask.nrelate.rt.utils.json;

/**
 * User: kaniyarasu
 * Date: 15/11/13
 * Time: 12:11 PM
 * JSON Validation details
 */
public class RequiredFields {
    private String fieldName;
    private String fieldType;
    private ValidationType validationType;

    public RequiredFields() {}

    public RequiredFields(String fieldName, String fieldType, ValidationType validationType) {
        this.fieldName = fieldName;
        this.fieldType = fieldType;
        this.validationType = validationType;
    }

    public ValidationType getValidationType() {
        return validationType;
    }

    public void setValidationType(ValidationType validationType) {
        this.validationType = validationType;
    }

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    public String getFieldType() {
        return fieldType;
    }

    public void setFieldType(String fieldType) {
        this.fieldType = fieldType;
    }
}
