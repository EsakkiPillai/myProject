package com.ask.nrelate.rt.utils.json;

import com.google.gson.Gson;

/**
 * Created by IntelliJ IDEA.
 * User: kaniyarasu
 * Date: 14/11/13
 * Time: 5:17 PM
 * Base class handles the request from the client.
 */
public class MarvelJson {
    public static <T> T fromJson(String jsonInput, Class<T> clazz) throws InstantiationException, IllegalAccessException {
        Gson gson = MarvelJsonBuilder.create(clazz);
        return gson.fromJson(jsonInput, clazz);
    }
}
