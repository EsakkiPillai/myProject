package com.ask.nrelate.rt.pojo;

import com.ask.nrelate.rt.utils.LogStatus;

/**
 * Created by IntelliJ IDEA.
 * User: kaniyarasu
 * Date: 11/11/13
 * Time: 12:13 PM
 * To change this template use File | Settings | File Templates.
 */
public class MapperOutput {
    private String ipAddress = "#";
    private String logDate = "#";
    private String eventType = "#";
    private String pluginType = "#";
    private String userAgent = "#";
    private String domain = "#";
    private String specificDomain = "#";
    private String sourceURL = "#";
    private String destinationURL = "#";
    private String destinationDomain = "#";
    private int cid = 0;
    private double cpc = 0.0;
    private double rpc = 0.0;
    private int widgetID = 0;
    private String geoCountry = "#";
    private String pageTypeID = "0";
    private String keywords = "#";
    private String logValue = "#";
    private String errorMessage = "#";
    private String logStatus = LogStatus.VALID.fieldName();
    private long domainID = 0L;
    private String prID = "#";
    private String platform = "#";
    private int noOfAds = 0;
    private int noOfInternal = 0;
    private int noOfExternal = 0;
    private long requestTS = 0L;
    private String webServerIP = "#";
    private String opedid = "#";
    private String eventID = "#";

    public String getEventID() {
        return eventID;
    }

    public void setEventID(String eventID) {
        this.eventID = eventID;
    }

    public String getOpedid() {
        return opedid;
    }

    public void setOpedid(String opedid) {
        this.opedid = opedid;
    }

    public int getCid() {
        return cid;
    }

    public void setCid(int cid) {
        this.cid = cid;
    }

    public double getCpc() {
        return cpc;
    }

    public void setCpc(double cpc) {
        this.cpc = cpc;
    }

    public String getDestinationDomain() {
        return destinationDomain;
    }

    public void setDestinationDomain(String destinationDomain) {
        this.destinationDomain = destinationDomain;
    }

    public String getDestinationURL() {
        return destinationURL;
    }

    public void setDestinationURL(String destinationURL) {
        this.destinationURL = destinationURL;
    }

    public String getDomain() {
        return domain;
    }

    public void setDomain(String domain) {
        this.domain = domain;
    }

    public long getDomainID() {
        return domainID;
    }

    public void setDomainID(long domainID) {
        this.domainID = domainID;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    public String getGeoCountry() {
        return geoCountry;
    }

    public void setGeoCountry(String geoCountry) {
        this.geoCountry = geoCountry;
    }

    public String getIpAddress() {
        return ipAddress;
    }

    public void setIpAddress(String ipAddress) {
        this.ipAddress = ipAddress;
    }

    public String getKeywords() {
        return keywords;
    }

    public void setKeywords(String keywords) {
        this.keywords = keywords;
    }

    public String getLogDate() {
        return logDate;
    }

    public void setLogDate(String logDate) {
        this.logDate = logDate;
    }

    public String getLogStatus() {
        return logStatus;
    }

    public void setLogStatus(String logStatus) {
        this.logStatus = logStatus;
    }

    public String getLogValue() {
        return logValue;
    }

    public void setLogValue(String logValue) {
        this.logValue = logValue;
    }

    public int getNoOfAds() {
        return noOfAds;
    }

    public void setNoOfAds(int noOfAds) {
        this.noOfAds = noOfAds;
    }

    public int getNoOfExternal() {
        return noOfExternal;
    }

    public void setNoOfExternal(int noOfExternal) {
        this.noOfExternal = noOfExternal;
    }

    public int getNoOfInternal() {
        return noOfInternal;
    }

    public void setNoOfInternal(int noOfInternal) {
        this.noOfInternal = noOfInternal;
    }

    public String getPageTypeID() {
        return pageTypeID;
    }

    public void setPageTypeID(String pageTypeID) {
        this.pageTypeID = pageTypeID;
    }

    public String getPlatform() {
        return platform;
    }

    public void setPlatform(String platform) {
        this.platform = platform;
    }

    public String getPluginType() {
        return pluginType;
    }

    public void setPluginType(String pluginType) {
        this.pluginType = pluginType;
    }

    public String getPrID() {
        return prID;
    }

    public void setPrID(String prID) {
        this.prID = prID;
    }

    public long getRequestTS() {
        return requestTS;
    }

    public void setRequestTS(long requestTS) {
        this.requestTS = requestTS;
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public double getRpc() {
        return rpc;
    }

    public void setRpc(double rpc) {
        this.rpc = rpc;
    }

    public String getSourceURL() {
        return sourceURL;
    }

    public void setSourceURL(String sourceURL) {
        this.sourceURL = sourceURL;
    }

    public String getSpecificDomain() {
        return specificDomain;
    }

    public void setSpecificDomain(String specificDomain) {
        this.specificDomain = specificDomain;
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

    public int getWidgetID() {
        return widgetID;
    }

    public void setWidgetID(int widgetID) {
        this.widgetID = widgetID;
    }
}
