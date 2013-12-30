package com.ask.nrelate.lookup;

/**
 * Created by IntelliJ IDEA.
 * User: kaniyarasu
 * Date: 13/3/13
 * Time: 1:16 PM
 * To change this template use File | Settings | File Templates.
 */
public interface Lookup {
    public String getRPC(String property);
    public String getDomainid(String property);
    public String getCPC(String property);
    public String getWidgetConfiguration(String property);
    public String getWidgetConfigurationWithoutWidgetid(String property);
    public String getPageType(String property);
    public String getRPCPercent(String property);
    public boolean isBillableURL(String pixelDate, String sourceURL);
    public String getPageTypeOverride(String property);
    public boolean isFraudulentIP(String ipAddress, String userAgent);
}
