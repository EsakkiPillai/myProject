package com.ask.nrelate.pojo;

/**
 * POJO for Mapper Output(for both RON and NRT)
 */
public class MapperOutput {

    private String ipAddress = "NAAAAA";
    private String logDate = "NA";
    private String timeZone = "NA";
    private String requestType = "NA";
    private String requestURL = "NA";
    private String clickType = "NA";
    private String pluginType = "NA";
    private String responseCode = "NA";
    private String responseSize = "NA";
    private String referrer = "NA";
    private String userAgent = "NA";
    private String decodedURL = "NA";
    private String domain = "NA";
    private String specificDomain = "NA";
    private String sourceURL = "NA";
    private String destinationURL = "NA";
    private String destinationDomain = "NA";
    private String cid = "0";
    private double cpc = 0.0;
    private double rpc = 0.0;
    private String widgetID = "0";
    private String pageType = "NA";
    private String reportGroup = "NA";
    private String geoCountry = "NA";
    private String geoDefault = "NA";
    private String geoCity = "NA";
    private String userID = "NA";
    private String sessionID = "NA";
    private String requestID = "NA";
    private String pageTypeID = "0";
    private String articleID = "NA";
    private String keywords = "NA";
    private String responseTime = "0";
    private boolean newUser = false;
    private String logValue = "NA";
    private String errorMessage = "NA";
    private String logStatus = "Invalid";
    private String contractStatus = "NA";
    private String pageTypeDerived = "NA";
    private int domainID = 0;
    private int noOfAds = 0;
    private int noOfRelatedPosts = 0;
    private int adOpt = 0;
    private String adDomain = "NA";
    private String prID = "NA";
    private int topPosition = 0;
    private int leftPosition = 0;
    private boolean logValid = true;
    private long requestTS = 0L;
    private boolean billable = true;
    private String platform = "NA";
    private String orgSourceURL = "NA";

    public MapperOutput() {
    }

    public String getOrgSourceURL() {
        return orgSourceURL;
    }

    public void setOrgSourceURL(String orgSourceURL) {
        this.orgSourceURL = orgSourceURL;
    }

    public String getPlatform() {
        return platform;
    }

    public void setPlatform(String platform) {
        this.platform = platform;
    }

    public boolean isBillable() {
        return billable;
    }

    public void setBillable(boolean billable) {
        billable = billable;
    }

    public long getRequestTS() {
        return requestTS;
    }

    public void setRequestTS(long requestTS) {
        this.requestTS = requestTS;
    }

    public boolean isLogValid() {
        return logValid;
    }

    public void setLogValid(boolean logValid) {
        this.logValid = logValid;
    }

    public String getIpAddress() {
        return ipAddress;
    }

    public void setIpAddress(String ipAddress) {
        this.ipAddress = ipAddress;
    }

    public String getLogDate() {
        return logDate;
    }

    public void setLogDate(String logDate) {
        this.logDate = logDate;
    }

    public String getTimeZone() {
        return timeZone;
    }

    public void setTimeZone(String timeZone) {
        this.timeZone = timeZone;
    }

    public String getRequestType() {
        return requestType;
    }

    public void setRequestType(String requestType) {
        this.requestType = requestType;
    }

    public String getRequestURL() {
        return requestURL;
    }

    public void setRequestURL(String requestURL) {
        this.requestURL = requestURL;
    }

    public String getClickType() {
        return clickType;
    }

    public void setClickType(String clickType) {
        this.clickType = clickType;
    }

    public String getPluginType() {
        return pluginType;
    }

    public void setPluginType(String pluginType) {
        this.pluginType = pluginType;
    }

    public String getResponseCode() {
        return responseCode;
    }

    public void setResponseCode(String responseCode) {
        this.responseCode = responseCode;
    }

    public String getResponseSize() {
        return responseSize;
    }

    public void setResponseSize(String responseSize) {
        this.responseSize = responseSize;
    }

    public String getReferrer() {
        return referrer;
    }

    public void setReferrer(String referrer) {
        this.referrer = referrer;
    }

    public String getUserAgent() {
        return userAgent;
    }

    public void setUserAgent(String userAgent) {
        this.userAgent = userAgent;
    }

    public String getDecodedURL() {
        return decodedURL;
    }

    public void setDecodedURL(String decodedURL) {
        this.decodedURL = decodedURL;
    }

    public String getDomain() {
        return domain;
    }

    public void setDomain(String domain) {
        this.domain = domain;
    }

    public String getSpecificDomain() {
        return specificDomain;
    }

    public void setSpecificDomain(String specificDomain) {
        this.specificDomain = specificDomain;
    }

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

    public String getDestinationDomain() {
        return destinationDomain;
    }

    public void setDestinationDomain(String destinationDomain) {
        this.destinationDomain = destinationDomain;
    }

    public String getCid() {
        return cid;
    }

    public void setCid(String cid) {
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

    public String getWidgetID() {
        return widgetID;
    }

    public void setWidgetID(String widgetID) {
        this.widgetID = widgetID;
    }

    public String getPageType() {
        return pageType;
    }

    public void setPageType(String pageType) {
        this.pageType = pageType;
    }

    public String getReportGroup() {
        return reportGroup;
    }

    public void setReportGroup(String reportGroup) {
        this.reportGroup = reportGroup;
    }

    public String getGeoCountry() {
        return geoCountry;
    }

    public void setGeoCountry(String geoCountry) {
        this.geoCountry = geoCountry;
    }

    public String getGeoDefault() {
        return geoDefault;
    }

    public void setGeoDefault(String geoDefault) {
        this.geoDefault = geoDefault;
    }

    public String getGeoCity() {
        return geoCity;
    }

    public void setGeoCity(String geoCity) {
        this.geoCity = geoCity;
    }

    public String getUserID() {
        return userID;
    }

    public void setUserID(String userID) {
        this.userID = userID;
    }

    public String getSessionID() {
        return sessionID;
    }

    public void setSessionID(String sessionID) {
        this.sessionID = sessionID;
    }

    public String getRequestID() {
        return requestID;
    }

    public void setRequestID(String requestID) {
        this.requestID = requestID;
    }

    public String getPageTypeID() {
        return pageTypeID;
    }

    public void setPageTypeID(String pageTypeID) {
        this.pageTypeID = pageTypeID;
    }

    public String getArticleID() {
        return articleID;
    }

    public void setArticleID(String articleID) {
        this.articleID = articleID;
    }

    public String getKeywords() {
        return keywords;
    }

    public void setKeywords(String keywords) {
        this.keywords = keywords;
    }

    public String getResponseTime() {
        return responseTime;
    }

    public void setResponseTime(String responseTime) {
        this.responseTime = responseTime;
    }

    public boolean isNewUser() {
        return newUser;
    }

    public void setNewUser(boolean newUser) {
        this.newUser = newUser;
    }

    public String getLogValue() {
        return logValue;
    }

    public void setLogValue(String logValue) {
        this.logValue = logValue;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    public String getLogStatus() {
        return logStatus;
    }

    public void setLogStatus(String logStatus) {
        this.logStatus = logStatus;
    }

    public String getContractStatus() {
        return contractStatus;
    }

    public void setContractStatus(String contractStatus) {
        this.contractStatus = contractStatus;
    }

    public String getPageTypeDerived() {
        return pageTypeDerived;
    }

    public void setPageTypeDerived(String pageTypeDerived) {
        this.pageTypeDerived = pageTypeDerived;
    }

    public int getDomainID() {
        return domainID;
    }

    public void setDomainID(int domainID) {
        this.domainID = domainID;
    }

    public int getNoOfAds() {
        return noOfAds;
    }

    public void setNoOfAds(int noOfAds) {
        this.noOfAds = noOfAds;
    }

    public int getNoOfRelatedPosts() {
        return noOfRelatedPosts;
    }

    public void setNoOfRelatedPosts(int noOfRelatedPosts) {
        this.noOfRelatedPosts = noOfRelatedPosts;
    }

    public int getAdOpt() {
        return adOpt;
    }

    public void setAdOpt(int adOpt) {
        this.adOpt = adOpt;
    }

    public String getAdDomain() {
        return adDomain;
    }

    public void setAdDomain(String adDomain) {
        this.adDomain = adDomain;
    }

    public String getPrID() {
        return prID;
    }

    public void setPrID(String prID) {
        this.prID = prID;
    }

    public int getTopPosition() {
        return topPosition;
    }

    public void setTopPosition(int topPosition) {
        this.topPosition = topPosition;
    }

    public int getLeftPosition() {
        return leftPosition;
    }

    public void setLeftPosition(int leftPosition) {
        this.leftPosition = leftPosition;
    }

    @Override
    public String toString() {
        return super.toString();
    }
}
