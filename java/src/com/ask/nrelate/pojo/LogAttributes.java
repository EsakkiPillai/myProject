package com.ask.nrelate.pojo;

import java.util.ArrayList;

/**
 * Created by IntelliJ IDEA.
 * User: kaniyarasu
 * Date: 12/3/13
 * Time: 1:09 PM
 * To change this template use File | Settings | File Templates.
 */
public class LogAttributes {
    private String remoteHost;
    private String identUser;
    private String authUser;
    private String date;
    private String time;
    private String timezone;
    private String method;
    private String url;
    private String protocol;
    private String status;
    private String bytes;
    private String referrer;
    private String userAgent;
    private String remoteIP;
    private String serverIP;
    private boolean logValid;

    public LogAttributes() {
    }

    public LogAttributes(boolean logValid){
        this.logValid = logValid;
    }

    public boolean isLogValid() {
        return logValid;
    }

    public void setLogValid(boolean logValid) {
        this.logValid = logValid;
    }

    public String getRemoteHost() {
        return remoteHost;
    }

    public void setRemoteHost(String remoteHost) {
        this.remoteHost = remoteHost;
    }

    public String getIdentUser() {
        return identUser;
    }

    public void setIdentUser(String identUser) {
        this.identUser = identUser;
    }

    public String getAuthUser() {
        return authUser;
    }

    public void setAuthUser(String authUser) {
        this.authUser = authUser;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }

    public String getTimezone() {
        return timezone;
    }

    public void setTimezone(String timezone) {
        this.timezone = timezone;
    }

    public String getMethod() {
        return method;
    }

    public void setMethod(String method) {
        this.method = method;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getProtocol() {
        return protocol;
    }

    public void setProtocol(String protocol) {
        this.protocol = protocol;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getBytes() {
        return bytes;
    }

    public void setBytes(String bytes) {
        this.bytes = bytes;
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

    public String getRemoteIP() {
        return remoteIP;
    }

    public void setRemoteIP(String remoteIP) {
        this.remoteIP = remoteIP;
    }

    public String getServerIP() {
        return serverIP;
    }

    public void setServerIP(String serverIP) {
        this.serverIP = serverIP;
    }
}
