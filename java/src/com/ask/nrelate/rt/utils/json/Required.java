package com.ask.nrelate.rt.utils.json;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * User: kaniyarasu
 * Date: 14/11/13
 * Time: 5:20 PM
 * JSON Field Validation - Annotate with @Required(fieldName = "Name in JSON",fieldType = "Expected Field Type")
 * FieldType should be with complete package. for eg: Integer should be java.lang.Integer and also do not include the
 * primitive data types.
 */
@Target(value = ElementType.FIELD)
@Retention(value = RetentionPolicy.RUNTIME)
public @interface Required {
    public String fieldName();
    public String fieldType();
    public ValidationType validationType();
}
