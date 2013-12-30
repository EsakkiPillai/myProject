package com.ask.nrelate.cassa;

/**
 * Created with IntelliJ IDEA.
 * User: sivakumar
 * Date: 27/11/13
 * Time: 12:49 PM
 * To change this template use File | Settings | File Templates.
 */
public interface CassandraUtils {
    public static String TABLE_NAME = "nrelate_data1";
    public static String KEYSPACE = "nrelate";
    public static String[] seeds = {"10.168.6.95"};
    public static String WIDGET_TABLE = "widget_data";
    public static String COUNTRY_TABLE = "country_data";
    public static String PLATFORM_TABLE = "platform_data";
}
