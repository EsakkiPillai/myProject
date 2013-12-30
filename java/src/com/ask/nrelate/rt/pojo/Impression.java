package com.ask.nrelate.rt.pojo;

import com.ask.nrelate.rt.utils.json.Required;
import com.ask.nrelate.rt.utils.json.ValidationType;
import com.google.gson.annotations.SerializedName;

import java.util.ArrayList;
import java.util.List;

/**
 * User: Kaniyarasu
 * Date: 22/1/13
 * Time: 4:52 PM
 */
public class Impression extends Common{

    //@Required(fieldName = "url", fieldType = "java.lang.String", validationType = ValidationType.REQ)
    @SerializedName("url")
    private String sourceURL="#";

    @SerializedName("int")
    private List<InternalImpression> internal = new ArrayList<InternalImpression>();

    @SerializedName("ad")
    private List<AdImpression> ad = new ArrayList<AdImpression>();


    public String getSourceURL() {
        return sourceURL;
    }

    public void setSourceURL(String sourceURL) {
        this.sourceURL = sourceURL;
    }

    public List<InternalImpression> getInternal() {
        return internal;
    }

    public void setInternal(List<InternalImpression> internal) {
        this.internal = internal;
    }

    public List<AdImpression> getAd() {
        return ad;
    }

    public void setAd(List<AdImpression> ad) {
        this.ad = ad;
    }

}