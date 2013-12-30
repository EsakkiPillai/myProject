package com.ask.nrelate.rt.utils.json;

import com.google.gson.*;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;

/**
 * User: kaniyarasu
 * Date: 14/11/13
 * Time: 5:18 PM
 * Custom JSON Deserializer for parsing and validating the JSON input.Here everything will be generic.
 * Version: 1
 */
public class MarvelJsonDeserializer implements JsonDeserializer{

    List<RequiredFields> requiredFields = new ArrayList<RequiredFields>();

    public void registerRequiredField(RequiredFields requiredField){
        requiredFields.add(requiredField);
    }

    public List<RequiredFields> getRequiredFields() {
        return requiredFields;
    }

    public void setRequiredFields(List<RequiredFields> requiredFields) {
        this.requiredFields = requiredFields;
    }

    @Override
    public Object deserialize(JsonElement jsonElement, Type type, JsonDeserializationContext jsonDeserializationContext) throws JsonParseException {
        JsonObject jsonObject = (JsonObject) jsonElement;
        for (RequiredFields requiredField : requiredFields) {
            if ((requiredField.getValidationType() == ValidationType.ALL
                    || requiredField.getValidationType() == ValidationType.REQ)
                    && jsonObject.get(requiredField.getFieldName()) == null)
                throw new JsonParseException("Required Field Not Found : " + requiredField.getFieldName());

            if(requiredField.getValidationType() == ValidationType.ALL
                    || requiredField.getValidationType() == ValidationType.TYPE){
                try {
                    if(jsonObject.get(requiredField.getFieldName()) != null){
                        if(requiredField.getFieldType().contains("Integer")){
                            Integer.parseInt(jsonObject.get(requiredField.getFieldName()).getAsString());
                        }else if(requiredField.getFieldType().contains("Double")){
                            Double.parseDouble(jsonObject.get(requiredField.getFieldName()).getAsString());
                        }else if(requiredField.getFieldType().contains("Long")){
                            Long.parseLong(jsonObject.get(requiredField.getFieldName()).getAsString());
                        }
                    }
                } catch (NumberFormatException e) {
                    throw new JsonParseException("Field Format Exception : " + requiredField.getFieldName());
                }
            }
        }

        return new Gson().fromJson(jsonElement, type);
    }
}
