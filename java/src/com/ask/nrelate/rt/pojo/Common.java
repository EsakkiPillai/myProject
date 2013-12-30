package com.ask.nrelate.rt.pojo;

import com.ask.nrelate.rt.utils.json.Required;
import com.ask.nrelate.rt.utils.json.ValidationType;
import com.google.gson.annotations.SerializedName;

/**
 * Created with IntelliJ IDEA.
 * User: sivakumar
 * Date: 12/12/13
 * Time: 4:03 PM
 * To change this template use File | Settings | File Templates.
 */
public class Common {

    @Required(fieldName = "cl_ip", fieldType = "java.lang.String", validationType = ValidationType.REQ)
    @SerializedName("cl_ip")
    private String ipAddress = "#";

    @Required(fieldName = "ts", fieldType = "java.lang.Long", validationType = ValidationType.ALL)
    @SerializedName("ts")
    private long requestTS=0;

    @Required(fieldName = "src_dom", fieldType = "java.lang.String", validationType = ValidationType.REQ)
    @SerializedName("src_dom")
    private String sourceDomain="#";

    @Required(fieldName = "src_did", fieldType = "java.lang.Long", validationType = ValidationType.ALL)
    @SerializedName("src_did")
    private long sourceDomainID=0;

    @Required(fieldName = "plugin", fieldType = "java.lang.String", validationType = ValidationType.REQ)
    private String plugin="#";

    @Required(fieldName = "widget_id", fieldType = "java.lang.Integer", validationType = ValidationType.TYPE)
    @SerializedName("widget_id")
    private int widgetID=0;

    @SerializedName("page_type_id")
    private String pageTypeID="0";

    @SerializedName("OPEDID")
    private String opedid = "#";

    @Required(fieldName = "ua", fieldType = "java.lang.String", validationType = ValidationType.REQ)
    @SerializedName("ua")
    private String userAgent = "#";

    @Required(fieldName = "ws_ip", fieldType = "java.lang.String", validationType = ValidationType.REQ)
    @SerializedName("ws_ip")
    private String webServerIP="#";

    @SerializedName("pr_id")
    private String prID="0";

    public String getIpAddress() {
        return ipAddress;
    }

    public void setIpAddress(String ipAddress) {
        this.ipAddress = ipAddress;
    }

    public long getRequestTS() {
        return requestTS;
    }

    public void setRequestTS(long requestTS) {
        this.requestTS = requestTS;
    }

    public String getSourceDomain() {
        return sourceDomain;
    }

    public void setSourceDomain(String sourceDomain) {
        this.sourceDomain = sourceDomain;
    }

    public long getSourceDomainID() {
        return sourceDomainID;
    }

    public void setSourceDomainID(long sourceDomainID) {
        this.sourceDomainID = sourceDomainID;
    }

    public String getPlugin() {
        return plugin;
    }

    public void setPlugin(String plugin) {
        this.plugin = plugin;
    }

    public int getWidgetID() {
        return widgetID;
    }

    public void setWidgetID(int widgetID) {
        this.widgetID = widgetID;
    }

    public String getPageTypeID() {
        return pageTypeID;
    }

    public void setPageTypeID(String pageTypeID) {
        this.pageTypeID = pageTypeID;
    }

    public String getOpedid() {
        return opedid;
    }

    public void setOpedid(String opedid) {
        this.opedid = opedid;
    }

    public String getUserAgent() {
        return userAgent;
    }

    public void setUserAgent(String userAgent) {
        this.userAgent = userAgent;
    }

    public String getWebServerIP() {
        return webServerIP;
    }

    public void setWebServerIP(String webServerIP) {
        this.webServerIP = webServerIP;
    }

    public String getPrID() {
        return prID;
    }

    public void setPrID(String prID) {
        this.prID = prID;
    }
}
