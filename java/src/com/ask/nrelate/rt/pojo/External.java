package com.ask.nrelate.rt.pojo;

import com.ask.nrelate.rt.utils.json.Required;
import com.ask.nrelate.rt.utils.json.ValidationType;
import com.google.gson.annotations.SerializedName;

/**
 * Created by IntelliJ IDEA.
 * User: kaniyarasu
 * Date: 22/1/13
 * Time: 6:57 PM
 * To change this template use File | Settings | File Templates.
 */
public class External extends Common{
    //@Required(fieldName = "src_url", fieldType = "java.lang.String", validationType = ValidationType.REQ)
    @SerializedName("src_url")
    private String sourceURL="#";

    @Required(fieldName = "dst_url", fieldType = "java.lang.String", validationType = ValidationType.REQ)
    @SerializedName("dst_url")
    private String destinationURL="#";

    public String getSourceURL() {
        return sourceURL;
    }

    public void setSourceURL(String sourceURL) {
        this.sourceURL = sourceURL;
    }

    public String getDestinationURL() {
        return destinationURL;
    }

    public void setDestinationURL(String destinationURL) {
        this.destinationURL = destinationURL;
    }
}
