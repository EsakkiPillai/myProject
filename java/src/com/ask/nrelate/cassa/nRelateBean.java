package com.ask.nrelate.cassa;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created with IntelliJ IDEA.
 * User: sivakumar
 * Date: 27/11/13
 * Time: 9:56 AM
 * To change this template use File | Settings | File Templates.
 */
public class nRelateBean {
    String clustertype;
    String key;
    String domain;
    long widgetid;
    String platform;
    String country;
    Date pixeldate;
    int pixelhour;
    long impressions;
    long paidimpressions;
    long ads;
    long internals;
    long externals;
    double grossRevenue;
    double netRevenue;
    float grossRPM;
    float netRPM;
    float adCTR;
    float internalCTR;

    public String getClustertype() {
        return clustertype;
    }

    public void setClustertype(String clustertype) {
        this.clustertype = clustertype;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getDomain() {
        return domain;
    }

    public void setDomain(String domain) {
        this.domain = domain;
    }

    public long getWidgetid() {
        return widgetid;
    }

    public void setWidgetid(long widgetid) {
        this.widgetid = widgetid;
    }

    public String getPlatform() {
        return platform;
    }

    public void setPlatform(String platform) {
        this.platform = platform;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public Date getPixeldate() {
        return pixeldate;
    }

    public void setPixeldate(Date pixeldate) {
            this.pixeldate = pixeldate;
    }

    public int getPixelhour() {
        return pixelhour;
    }

    public void setPixelhour(int pixelhour) {
        this.pixelhour = pixelhour;
    }

    public long getImpressions() {
        return impressions;
    }

    public void setImpressions(long impressions) {
        this.impressions = impressions;
    }

    public long getPaidimpressions() {
        return paidimpressions;
    }

    public void setPaidimpressions(long paidimpressions) {
        this.paidimpressions = paidimpressions;
    }

    public long getAds() {
        return ads;
    }

    public void setAds(long ads) {
        this.ads = ads;
    }

    public long getInternals() {
        return internals;
    }

    public void setInternals(long internals) {
        this.internals = internals;
    }

    public long getExternals() {
        return externals;
    }

    public void setExternals(long externals) {
        this.externals = externals;
    }

    public double getGrossRevenue() {
        return grossRevenue;
    }

    public void setGrossRevenue(double grossRevenue) {
        this.grossRevenue = grossRevenue;
    }

    public double getNetRevenue() {
        return netRevenue;
    }

    public void setNetRevenue(double netRevenue) {
        this.netRevenue = netRevenue;
    }

    public float getGrossRPM() {
        return grossRPM;
    }

    public void setGrossRPM(float grossRPM) {
        this.grossRPM = grossRPM;
    }

    public float getNetRPM() {
        return netRPM;
    }

    public void setNetRPM(float netRPM) {
        this.netRPM = netRPM;
    }

    public float getAdCTR() {
        return adCTR;
    }

    public void setAdCTR(float adCTR) {
        this.adCTR = adCTR;
    }

    public float getInternalCTR() {
        return internalCTR;
    }

    public void setInternalCTR(float internalCTR) {
        this.internalCTR = internalCTR;
    }
}

