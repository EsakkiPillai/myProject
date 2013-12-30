package com.ask.nrelate.rt.utils.json;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

/**
 * User: kaniyarasu
 * Date: 14/11/13
 * Time: 5:18 PM
 * Prepare the Deserializer with validation parameters
 */
public class MarvelJsonBuilder {
    public static <T> Gson create(Class<T> clazz) throws IllegalAccessException, InstantiationException {
        GsonBuilder gsonBuilder = new GsonBuilder();
        MarvelJsonDeserializer marvelJsonDeserializer = new MarvelJsonDeserializer();
        Object object = clazz.newInstance();
        marvelJsonDeserializer.setRequiredFields(prepareRequiredFields(object));
        gsonBuilder.registerTypeAdapter(object.getClass(), marvelJsonDeserializer);
        return gsonBuilder.create();

    }

    public static List<RequiredFields> prepareRequiredFields(Object object){
        Field[] fields = object.getClass().getDeclaredFields();
        List<RequiredFields> requiredFields = new ArrayList<RequiredFields>();

        for(Field field : fields){
            Required required = field.getAnnotation(Required.class);
            if(required != null){
                requiredFields.add(new RequiredFields(required.fieldName(), required.fieldType(), required.validationType()));
            }
        }
        return requiredFields;
    }
}
