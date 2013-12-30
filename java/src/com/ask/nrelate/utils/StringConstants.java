package com.ask.nrelate.utils;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public interface StringConstants {

    //Date Formats
    SimpleDateFormat logDateFormat = new SimpleDateFormat("dd-MMM-yyyy:HH:mm:ss");
    SimpleDateFormat hiveDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    String hiveDateFormat = "yyyy-MM-dd HH:mm:ss";

    // Valid Plugins
    List<String> plugins = new ArrayList<String>(){{add("rc");add("mp");add("fo");add("rs");}};
    // Valid clicktypes
    List<String> clicktypes =new ArrayList<String>(){{add("ad");add("external");add("internal");}};
    // ip filtering for internal ip's
    //List<String> ipInternal = new ArrayList<String>(){{add("71.19.242.241");add("74.3.167.5");add("71.19.246.51");add("71.19.240.8");add("")}};
    List<String> ipInternal = new ArrayList<String>(){{add("71.19.242.241");add("74.3.167.5");add("71.19.246.51");add("71.19.240.8");add("71.19.243.123");add("66.135.33.159");add("66.135.59.7");}};


    //Error Messages
    String logRegexErrorMsg = "Log doesn't satisfy the json format";
    String JsonFieldMissingErrorMsg = "Field Missing in Json";
    String invalidPlugin = "Invalid plugin found";
    String invalidClick = "Invalid click found";
    String logBotErrorMsg = "Log contains 'google web preview' or 'www.google.com/bot.html' or 'nologging=1' or 'nonjs=1'";
    String userAgentBLMsg = "User Agent Black Listed";
    String mpBLMsg = "Log contains 'loadcounter.php' or 'nocache=1'";
    String logWOUrlMsg = "Log doesnt contain url parameter";
    String logWODomainMsg = "Log doesnt contain 'domain' or 'track.html";
    String pbmRetrievingPluginMsg = "Problem while retrieving plugin details";
    String pluginNotFoundMsg = "Plugin not found in the log";
    String urlDecoderExceptionMsg = "URLDecoder Exception";
    String arrayIndexOutOfExceptionMsg = "Array Index Out Bound Of Exception";
    String nullQueryParameterMsg = "Null queryParameters";
    String noURLFoundErrorMsg = "No url found in regex match";
    String noSourceURLFoundErrorMsg = "No src_url found in regex match";
    String geoLookupExceptionMsg = "Look Up Service Exception";
    String invalidDomain = "Invalid Domain";
    String invalidPluginOrClick = "Invalid Plugin Type or ClickType";
    String validLogMsg = "Valid Log";
    String noRefFoundMsg = "No referred found";
    String QUESTION_MARK= "#QUESTION_MARK#";
    String demoEventMsg = "Demo Event";
    String nRelateInternalIP = "This is an internal ip of nrelate";

    //Possible bot content
    List<String> botContents = new ArrayList<String>(){{
        add("www.google.com/bot.html");
        add("google web preview");
        add("nologging=1");
        add("nonjs=1");
    }};
    List<String> mpBLKeywords = new ArrayList<String>(){{
        add("loadcounter.php");
        add("nocache=1");
    }};

    List<String> inContractCountries = new ArrayList<String>(){{
        add("GB");
        add("CA");
        add("AU");
        add("SG");
        add("FR");
    }};

    String badUsers[] = {"googlebot", "slurp", "nutch", "spider", "crawler", "robot", "bot", "archive", "feedfetcher-google", "preview", "expo9"};

    //Character Constants
    char colon = ':';
    String tab = "\t";
    String HASH = "#";

    //Log Patterns
    String reqDatePattern = "\\[([^:]+):(\\d+:\\d+:\\d+)";

    //Destination URL, list
    String[] youtubeAttrs = {"v=S251SfpMRkY", "v=9qP0HsrZHys", "v=OYE9hg12lH8", "v=JTpXGTTaQfY", "v=M2mhgcQnn",
            "v=jJ-CZGPCGQc",  "v=amUigbAH6Vg"};


    List<String> blackListVarForValidation = new ArrayList<String>(){{
        add("logvalue");
        add("url");
    }};

    String IP_ADDRESS = "ipaddress";
    String USER_AGENT="useragent";
    String DOMAIN = "domain";
    String REQUEST_TYPE = "requesttype";
    String TIME = "time";
    String PLUGIN = "plugin";
    String FRAUD_MAP_OUTPUT_DELIM = "|*|";
    
    List<String> invalidSourceURLPattern = new ArrayList<String>(){{
        add("null");
        add("https://twitter.com/share");
        add("javascript:;");
    }};
    
    String DEMO_REF="http://demo.nrelate.com";
    
    String LOG_STATUS_INVALID = "Invalid";
    
    String JSON_NORM_OUTPUT_DELIM = tab;
    String JSON_NORM_DEFAULT = HASH;
    String GEO_FILENAME="GeoIP.dat";
    String FRAUDULENT_FILENAME="fraudulentIPFile";
}
