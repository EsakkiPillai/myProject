package com.ask.nrelate.rt.pojo;

import com.ask.nrelate.rt.utils.json.Required;
import com.ask.nrelate.rt.utils.json.ValidationType;
import com.google.gson.annotations.SerializedName;

/**
 * Created with IntelliJ IDEA.
 * User: sivakumar
 * Date: 2/12/13
 * Time: 4:52 PM
 * To change this template use File | Settings | File Templates.
 */
public class Timestamp {
    @Required(fieldName = "ts", fieldType = "java.lang.Long", validationType = ValidationType.ALL)
    @SerializedName("ts")
    private long requestTS=0;

    public long getRequestTS() {
        return requestTS;
    }

    public void setRequestTS(long requestTS) {
        this.requestTS = requestTS;
    }
}
