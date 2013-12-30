package com.ask.nrelate.rt.pojo;

import com.ask.nrelate.rt.utils.LogStatus;

/**
 * Created with IntelliJ IDEA.
 * User: sivakumar
 * Date: 26/12/13
 * Time: 1:38 PM
 * To change this template use File | Settings | File Templates.
 */
public class AdImpressionOutput {
    private String logDate = "#";
    private String domain = "#";
    private String sourceURL = "#";
    private String destinationURL = "#";
    private long cid = 0;
    private String logValue = "#";
    private long requestTS=0;

    public long getRequestTS() {
        return requestTS;
    }

    public void setRequestTS(long requestTS) {
        this.requestTS = requestTS;
    }

    public String getLogDate() {
        return logDate;
    }

    public void setLogDate(String logDate) {
        this.logDate = logDate;
    }

    public String getDomain() {
        return domain;
    }

    public void setDomain(String domain) {
        this.domain = domain;
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

    public long getCid() {
        return cid;
    }

    public void setCid(long cid) {
        this.cid = cid;
    }

    public String getLogValue() {
        return logValue;
    }

    public void setLogValue(String logValue) {
        this.logValue = logValue;
    }
}
