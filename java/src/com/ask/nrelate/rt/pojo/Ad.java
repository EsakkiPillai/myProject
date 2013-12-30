package com.ask.nrelate.rt.pojo;

import com.ask.nrelate.rt.utils.json.Required;
import com.ask.nrelate.rt.utils.json.ValidationType;
import com.google.gson.annotations.SerializedName;

/**
 * Created by IntelliJ IDEA.
 * User: kaniyarasu
 * Date: 22/1/13
 * Time: 6:58 PM
 * To change this template use File | Settings | File Templates.
 */
public class Ad extends Common{

    @Required(fieldName = "src_url", fieldType = "java.lang.String", validationType = ValidationType.REQ)
    @SerializedName("src_url")
    private String sourceURL="#";

    @Required(fieldName = "dst_url", fieldType = "java.lang.String", validationType = ValidationType.REQ)
    @SerializedName("dst_url")
    private String destinationURL="#";

    @Required(fieldName = "cid", fieldType = "java.lang.Double", validationType = ValidationType.ALL)
    private double cid=0;

    @Required(fieldName = "cpc", fieldType = "java.lang.Double", validationType = ValidationType.ALL)
    private double cpc=0;

    @Required(fieldName = "rpc", fieldType = "java.lang.Double", validationType = ValidationType.ALL)
    private double rpc=0;

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

    public double getCid() {
        return cid;
    }

    public void setCid(double cid) {
        this.cid = cid;
    }

    public double getCpc() {
        return cpc;
    }

    public void setCpc(double cpc) {
        this.cpc = cpc;
    }

    public double getRpc() {
        return rpc;
    }

    public void setRpc(double rpc) {
        this.rpc = rpc;
    }
}
