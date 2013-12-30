package com.ask.nrelate.rt.utils;

import com.ask.nrelate.rt.pojo.*;
import com.ask.nrelate.utils.DeviceType;
import com.ask.nrelate.utils.StringConstants;
import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;
import com.maxmind.geoip.LookupService;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import java.math.BigInteger;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLDecoder;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

/**
 * Created by IntelliJ IDEA.
 * User: kaniyarasu
 * Date: 6/11/13
 * Time: 5:55 PM
 * To change this template use File | Settings | File Templates.
 */
public class RTMapperUtils {

    /**
     * Method parse the JSON input, which identify JSON log type
     * and return the RTLogType(Impression, Ad, Internal, External)
     * @param jsonInput
     * @return
     */
    public static com.ask.nrelate.rt.utils.RTLogType getRTLogType(String jsonInput){
        Gson gson = new Gson();
        RTValidation rtValidation = gson.fromJson(jsonInput, RTValidation.class);

        if(rtValidation.getType() != null)
            return rtValidation.getType().equals(RTLogType.AD.fieldName()) ?
                        com.ask.nrelate.rt.utils.RTLogType.AD
                        : rtValidation.getType().equals(RTLogType.EXT.fieldName())
                            ? com.ask.nrelate.rt.utils.RTLogType.EXT
                            : rtValidation.getType().equals(RTLogType.INT.fieldName())
                                ? com.ask.nrelate.rt.utils.RTLogType.INT
                                : null;

        return com.ask.nrelate.rt.utils.RTLogType.IMPRESSION;
    }

    /**
     * Converting the impression object to the Mapper Output format
     * @param impression
     * @return
     */
    public static MapperOutput convertImpressionToMapperOutput(Impression impression){
        MapperOutput mapperOutput = new MapperOutput();
        //Assign impression fields to MapperOutput fields.
        mapperOutput.setDomain(
                normalizeDomain(
                        impression.getSourceDomain()
                ));
        mapperOutput.setDomainID(impression.getSourceDomainID());
        mapperOutput.setSourceURL(impression.getSourceURL());
        mapperOutput.setIpAddress(impression.getIpAddress());
        mapperOutput.setEventType(EventType.Impression.fieldName());
        mapperOutput.setPluginType(impression.getPlugin());
        mapperOutput.setWidgetID(impression.getWidgetID());
        mapperOutput.setWebServerIP(impression.getWebServerIP());
        mapperOutput.setUserAgent(impression.getUserAgent().replaceAll("\\t"," "));
        mapperOutput.setPageTypeID(impression.getPageTypeID());
        mapperOutput.setRequestTS(impression.getRequestTS());
        mapperOutput.setPrID(impression.getPrID());
        mapperOutput.setLogStatus(LogStatus.VALID.fieldName());

        //Updates number of ads, internal and external content
        mapperOutput = RTMapperUtils.updateContentCounts(mapperOutput, impression);
        return mapperOutput;
    }

    /**
     * Converting the AdImpression object to the Mapper Output format
     * @param adImpression
     * @param impression
     * @return
     * @throws MalformedURLException
     */
    public static MapperOutput convertAdImpressionToMapperOutput(AdImpression adImpression, Impression impression) throws MalformedURLException {
        MapperOutput mapperOutput = new MapperOutput();
        mapperOutput.setDestinationURL(adImpression.getAdURL());
        mapperOutput.setDestinationDomain(normalizeDomain(
                getDomainFromURL(adImpression.getAdURL())
        ));
        mapperOutput.setCid((int)adImpression.getCid());
        mapperOutput.setCpc(adImpression.getCpc());
        mapperOutput.setOpedid(adImpression.getOpedid());
        mapperOutput.setEventType(EventType.AdImpression.fieldName());
        mapperOutput.setRequestTS(impression.getRequestTS());
        mapperOutput.setLogStatus(LogStatus.VALID.fieldName());
        return mapperOutput;
    }

    public static AdImpressionOutput convertAdImpressionToMapperOutput_v2(AdImpression adImpression, Impression impression) throws MalformedURLException {
        AdImpressionOutput adImpressionOutput = new AdImpressionOutput();
        adImpressionOutput.setRequestTS(impression.getRequestTS());
        adImpressionOutput.setSourceURL(impression.getSourceURL());
        adImpressionOutput.setDestinationURL(adImpression.getAdURL());
        adImpressionOutput.setDomain(impression.getSourceDomain());
        adImpressionOutput.setCid((long)adImpression.getCid());
        return adImpressionOutput;
    }

    /**
     * Converting the InternalImpression and ExternalImpression object to the Mapper Output format
     * @param internalImpression
     * @param impression
     * @return
     */
    public static MapperOutput convertIntImpressionToMapperOutput(InternalImpression internalImpression, Impression impression){
        MapperOutput mapperOutput = new MapperOutput();
        mapperOutput.setDestinationURL(internalImpression.getInternalURL());
        mapperOutput.setOpedid(internalImpression.getOpedid());
        mapperOutput.setEventType(internalImpression.getEventType().fieldName());
        mapperOutput.setRequestTS(impression.getRequestTS());
        mapperOutput.setLogStatus(LogStatus.VALID.fieldName());
        return mapperOutput;
    }

    public static Text convertInternalImpressionToMapperOutput(MapperOutput mapperOutput){

        return new Text(mapperOutput.getLogDate()+StringConstants.tab+
                        mapperOutput.getCid()+StringConstants.tab+
                        mapperOutput.getDomain()+StringConstants.tab+
                        mapperOutput.getRequestTS()+StringConstants.tab+
                        mapperOutput.getSourceURL()+StringConstants.tab+
                        mapperOutput.getDestinationURL()+StringConstants.tab);
    }

    /**
     * Converting the Ad Click details to the Mapper Output format
     * @param ad
     * @return
     * @throws MalformedURLException
     */
    public static MapperOutput convertAdToMapperOutput(Ad ad) throws MalformedURLException {
        MapperOutput mapperOutput = new MapperOutput();
        mapperOutput.setIpAddress(ad.getIpAddress());
        mapperOutput.setUserAgent(ad.getUserAgent());
        mapperOutput.setPluginType(ad.getPlugin());
        mapperOutput.setDomain(normalizeDomain(ad.getSourceDomain()));
        if(mapperOutput.getDomain()== null || mapperOutput.getDomain().equals("#") || mapperOutput.getDomain().equals(":") ||
            mapperOutput.getDomain().isEmpty()){
                mapperOutput.setDomain(
                    normalizeDomain(
                        getDomainFromURL(ad.getSourceURL())
                    )
                );
        }
        mapperOutput.setSourceURL(ad.getSourceURL());
        mapperOutput.setDestinationDomain(
                normalizeDomain(
                        getDomainFromURL(ad.getDestinationURL())
                )
        );
        mapperOutput.setDestinationURL(ad.getDestinationURL());
        mapperOutput.setCid((int)ad.getCid());
        mapperOutput.setOpedid(ad.getOpedid());
        mapperOutput.setDomainID(ad.getSourceDomainID());
        mapperOutput.setCpc(ad.getCpc());
        mapperOutput.setRpc(ad.getRpc());
        mapperOutput.setWidgetID(ad.getWidgetID());
        mapperOutput.setWebServerIP(ad.getWebServerIP());
        mapperOutput.setRequestTS(ad.getRequestTS());
        mapperOutput.setEventType(EventType.AdClick.fieldName());
        mapperOutput.setLogStatus(LogStatus.VALID.fieldName());
        return mapperOutput;
    }

    public static Text convertAdImpressionToMapperOutput(AdImpressionOutput adImpressionOutput){

        return new Text(adImpressionOutput.getLogDate()+StringConstants.tab+
                adImpressionOutput.getDomain()+StringConstants.tab+
                (long)adImpressionOutput.getCid()+StringConstants.tab+
                //adImpressionOutput.getRequestTS()+StringConstants.tab+
                adImpressionOutput.getSourceURL()+StringConstants.tab+
                adImpressionOutput.getDestinationURL()+StringConstants.tab+
                adImpressionOutput.getLogValue());
    }

    /**
     * Converting the Internal Click details to the Mapper Output format
     * @param internal
     * @return
     * @throws MalformedURLException
     */
    public static MapperOutput convertInternalToMapperOutput(Internal internal) throws MalformedURLException {
        MapperOutput mapperOutput = new MapperOutput();
        mapperOutput.setPluginType(internal.getPlugin());
        mapperOutput.setDomain(
                normalizeDomain(
                        internal.getSourceDomain()
                ));
        mapperOutput.setSourceURL(internal.getSourceURL());
        mapperOutput.setDestinationDomain(
                normalizeDomain(
                        getDomainFromURL(internal.getDestinationURL())
                )
        );
        mapperOutput.setDestinationURL(internal.getDestinationURL());
        mapperOutput.setOpedid(internal.getOpedid());
        mapperOutput.setWidgetID(internal.getWidgetID());
        mapperOutput.setPageTypeID(internal.getPageTypeID());
        mapperOutput.setRequestTS(internal.getRequestTS());
        mapperOutput.setPrID(internal.getPrID());
        mapperOutput.setWebServerIP(internal.getWebServerIP());
        mapperOutput.setDomainID(internal.getSourceDomainID());
        mapperOutput.setUserAgent(internal.getUserAgent());
        mapperOutput.setIpAddress(internal.getIpAddress());
        mapperOutput.setEventType(EventType.InternalClick.fieldName());
        mapperOutput.setLogStatus(LogStatus.VALID.fieldName());
        return mapperOutput;
    }

    /**
     * Converting the External Click details to the Mapper Output format
     * @param external
     * @return
     * @throws MalformedURLException
     */
    public static MapperOutput convertExternalToMapperOutput(External external) throws MalformedURLException {
        MapperOutput mapperOutput = new MapperOutput();
        mapperOutput.setPluginType(external.getPlugin());
        mapperOutput.setDomain(
                normalizeDomain(
                        external.getSourceDomain()
                ));
        mapperOutput.setSourceURL(external.getSourceURL());
        mapperOutput.setDestinationDomain(
                normalizeDomain(
                        getDomainFromURL(external.getDestinationURL())
                )
        );
        mapperOutput.setDestinationURL(external.getDestinationURL());
        mapperOutput.setOpedid(external.getOpedid());
        mapperOutput.setWidgetID(external.getWidgetID());
        mapperOutput.setPageTypeID(external.getPageTypeID());
        mapperOutput.setRequestTS(external.getRequestTS());
        mapperOutput.setPrID(external.getPrID());
        mapperOutput.setWebServerIP(external.getWebServerIP());
        mapperOutput.setDomainID(external.getSourceDomainID());
        mapperOutput.setUserAgent(external.getUserAgent());
        mapperOutput.setIpAddress(external.getIpAddress());
        mapperOutput.setEventType(EventType.ExternalClick.fieldName());
        mapperOutput.setLogStatus(LogStatus.VALID.fieldName());
        return mapperOutput;
    }

    /**
     * Identify the device type based on the client user agent, Output will be type DeviceType
     * @param userAgent
     * @return
     */
    public static String getPlatform(String userAgent) {
        userAgent = userAgent.toLowerCase();

        if((userAgent.length() > 4) && (userAgent.matches("(?i).*((android|bb\\d+|meego).+mobile|android|mobi|avantgo|bada\\/|blackberry|blazer|compal|elaine|fennec|hiptop|iemobile|ip(hone|od|ad)|iris|kindle|lge |maemo|midp|mmp|htc|tablet|netfront|opera m(ob|in)i|palm( os)?|phone|p(ixi|re)\\/|plucker|pocket|psp|series(4|6)0|symbian|treo|up\\.(browser|link)|vodafone|wap|windows (ce|phone)|xda|xiino).*")||
                userAgent.substring(0,4).matches("(?i)1207|6310|6590|3gso|4thp|50[1-6]i|770s|802s|a wa|abac|ac(er|oo|s\\-)|ai(ko|rn)|al(av|ca|co)|amoi|an(ex|ny|yw)|aptu|ar(ch|go)|as(te|us)|attw|au(di|\\-m|r |s )|avan|be(ck|ll|nq)|bi(lb|rd)|bl(ac|az)|br(e|v)w|bumb|bw\\-(n|u)|c55\\/|capi|ccwa|cdm\\-|cell|chtm|cldc|cmd\\-|co(mp|nd)|craw|da(it|ll|ng)|dbte|dc\\-s|devi|dica|dmob|do(c|p)o|ds(12|\\-d)|el(49|ai)|em(l2|ul)|er(ic|k0)|esl8|ez([4-7]0|os|wa|ze)|fetc|fly(\\-|_)|g1 u|g560|gene|gf\\-5|g\\-mo|go(\\.w|od)|gr(ad|un)|haie|hcit|hd\\-(m|p|t)|hei\\-|hi(pt|ta)|hp( i|ip)|hs\\-c|ht(c(\\-| |_|a|g|p|s|t)|tp)|hu(aw|tc)|i\\-(20|go|ma)|i230|iac( |\\-|\\/)|ibro|idea|ig01|ikom|im1k|inno|ipaq|iris|ja(t|v)a|jbro|jemu|jigs|kddi|keji|kgt( |\\/)|klon|kpt |kwc\\-|kyo(c|k)|le(no|xi)|lg( g|\\/(k|l|u)|50|54|\\-[a-w])|libw|lynx|m1\\-w|m3ga|m50\\/|ma(te|ui|xo)|mc(01|21|ca)|m\\-cr|me(rc|ri)|mi(o8|oa|ts)|mmef|mo(01|02|bi|de|do|t(\\-| |o|v)|zz)|mt(50|p1|v )|mwbp|mywa|n10[0-2]|n20[2-3]|n30(0|2)|n50(0|2|5)|n7(0(0|1)|10)|ne((c|m)\\-|on|tf|wf|wg|wt)|nok(6|i)|nzph|o2im|op(ti|wv)|oran|owg1|p800|pan(a|d|t)|pdxg|pg(13|\\-([1-8]|c))|phil|pire|pl(ay|uc)|pn\\-2|po(ck|rt|se)|prox|psio|pt\\-g|qa\\-a|qc(07|12|21|32|60|\\-[2-7]|i\\-)|qtek|r380|r600|raks|rim9|ro(ve|zo)|s55\\/|sa(ge|ma|mm|ms|ny|va)|sc(01|h\\-|oo|p\\-)|sdk\\/|se(c(\\-|0|1)|47|mc|nd|ri)|sgh\\-|shar|sie(\\-|m)|sk\\-0|sl(45|id)|sm(al|ar|b3|it|t5)|so(ft|ny)|sp(01|h\\-|v\\-|v )|sy(01|mb)|t2(18|50)|t6(00|10|18)|ta(gt|lk)|tcl\\-|tdg\\-|tel(i|m)|tim\\-|t\\-mo|to(pl|sh)|ts(70|m\\-|m3|m5)|tx\\-9|up(\\.b|g1|si)|utst|v400|v750|veri|vi(rg|te)|vk(40|5[0-3]|\\-v)|vm40|voda|vulc|vx(52|53|60|61|70|80|81|83|85|98)|w3c(\\-| )|webc|whit|wi(g |nc|nw)|wmlb|wonu|x700|yas\\-|your|zeto|zte\\-"))) {
            if(userAgent.indexOf("android") != -1 && (userAgent.indexOf("mobile") == -1 || userAgent.indexOf("tablet") != -1)){
                return DeviceType.Tablet.toString();
            }

            if(userAgent.indexOf("ipad") != -1 || userAgent.indexOf("tablet") != -1){
                return DeviceType.Tablet.toString();
            }

            return DeviceType.Mobile.toString();
        }
        return DeviceType.Desktop.toString();
    }

    /**
     * Converting the Epoch timestamp to the Hive Date Format, which will be in UTC/GMT
     * @param timeStamp
     * @return
     */
    public static String generateHiveDate(long timeStamp){
        SimpleDateFormat hiveDateFormat = new SimpleDateFormat(StringConstants.hiveDateFormat);
        hiveDateFormat.setTimeZone(TimeZone.getTimeZone("Etc/UTC"));
        return hiveDateFormat.format(new Date(timeStamp));
    }

    /**
     * Geo Lookup - Country code will be returned based on the Client IPAddress.
     * @param geoLookupService
     * @param ipValue
     * @return
     */
    public static String getCountry(LookupService geoLookupService, String ipValue){
        String country = StringConstants.JSON_NORM_DEFAULT;
        if(!ipValue.equals("-")){
            if(ipValue.startsWith(",")){
                ipValue = ipValue.substring(1).trim();
                country = geoLookupService.getCountry(ipValue).getCode();
            }else if(ipValue.contains(",")){
                String multipleIPList[] = ipValue.split(",");
                if (multipleIPList != null && multipleIPList.length > 0) {
                    String candidateIP = multipleIPList [0];
                    country = geoLookupService.getCountry(candidateIP).getCode();
                }
            }else {
                country = geoLookupService.getCountry(ipValue).getCode();
            }
        }
        return country;
    }

    /**
     * Generates the Mapper Output, structure will be similar to immediate hive table().
     * Each field will be separated by tab character(\t) and default values will be set Hash(#)
     * @param mapperOutput
     * @return
     */
    public static String constructMapperOutput(MapperOutput mapperOutput){
        return mapperOutput.getIpAddress() + StringConstants.JSON_NORM_OUTPUT_DELIM
                + mapperOutput.getLogDate() + StringConstants.JSON_NORM_OUTPUT_DELIM
                + mapperOutput.getEventType() + StringConstants.JSON_NORM_OUTPUT_DELIM
                + mapperOutput.getPluginType() + StringConstants.JSON_NORM_OUTPUT_DELIM
                + mapperOutput.getUserAgent() + StringConstants.JSON_NORM_OUTPUT_DELIM
                + mapperOutput.getDomain() + StringConstants.JSON_NORM_OUTPUT_DELIM
                + mapperOutput.getSpecificDomain() + StringConstants.JSON_NORM_OUTPUT_DELIM
                + mapperOutput.getSourceURL() + StringConstants.JSON_NORM_OUTPUT_DELIM
                + mapperOutput.getDestinationURL() + StringConstants.JSON_NORM_OUTPUT_DELIM
                + mapperOutput.getDestinationDomain() + StringConstants.JSON_NORM_OUTPUT_DELIM
                + (long)mapperOutput.getCid() + StringConstants.JSON_NORM_OUTPUT_DELIM
                + mapperOutput.getCpc() + StringConstants.JSON_NORM_OUTPUT_DELIM
                + mapperOutput.getRpc() + StringConstants.JSON_NORM_OUTPUT_DELIM
                + mapperOutput.getWidgetID() + StringConstants.JSON_NORM_OUTPUT_DELIM
                + mapperOutput.getGeoCountry() + StringConstants.JSON_NORM_OUTPUT_DELIM
                + mapperOutput.getPageTypeID() + StringConstants.JSON_NORM_OUTPUT_DELIM
                + mapperOutput.getKeywords() + StringConstants.JSON_NORM_OUTPUT_DELIM
                + mapperOutput.getLogValue() + StringConstants.JSON_NORM_OUTPUT_DELIM
                + mapperOutput.getErrorMessage() + StringConstants.JSON_NORM_OUTPUT_DELIM
                + mapperOutput.getLogStatus() + StringConstants.JSON_NORM_OUTPUT_DELIM
                + mapperOutput.getDomainID() + StringConstants.JSON_NORM_OUTPUT_DELIM
                + mapperOutput.getPrID() + StringConstants.JSON_NORM_OUTPUT_DELIM
                + mapperOutput.getPlatform() + StringConstants.JSON_NORM_OUTPUT_DELIM
                + mapperOutput.getNoOfAds() + StringConstants.JSON_NORM_OUTPUT_DELIM
                + mapperOutput.getNoOfInternal() + StringConstants.JSON_NORM_OUTPUT_DELIM
                + mapperOutput.getNoOfExternal() + StringConstants.JSON_NORM_OUTPUT_DELIM
                + mapperOutput.getRequestTS() + StringConstants.JSON_NORM_OUTPUT_DELIM
                + mapperOutput.getWebServerIP() + StringConstants.JSON_NORM_OUTPUT_DELIM
                + mapperOutput.getOpedid() + StringConstants.JSON_NORM_OUTPUT_DELIM
                + mapperOutput.getEventID();
    }

    /**
     * Update the Hive Date Format, Platform and GeoCountry
     * @param geoLookupService
     * @param mapperOutput
     * @return
     */
    public static MapperOutput generatePTC(LookupService geoLookupService, MapperOutput mapperOutput){
        mapperOutput.setLogDate(RTMapperUtils.generateHiveDate(mapperOutput.getRequestTS()));
        mapperOutput.setPlatform(RTMapperUtils.getPlatform(mapperOutput.getUserAgent()));
        mapperOutput.setGeoCountry(RTMapperUtils.getCountry(geoLookupService, mapperOutput.getIpAddress()));
        return mapperOutput;
    }


    /**
     * Update the AdImpression, InternalImpression and ExternalImpression count to the impression record.
     * @param mapperOutput
     * @param impression
     * @return
     */
    public static MapperOutput updateContentCounts(MapperOutput mapperOutput, Impression impression){
        int internalCount = 0 ,externalCount = 0;
        if(impression.getAd() != null)
            mapperOutput.setNoOfAds(impression.getAd().size());
        if(impression.getInternal() != null){
            for(InternalImpression internalImpression : impression.getInternal()){
                if(internalImpression.getEventType().equals(EventType.InternalImpression))
                    internalCount++;
                else
                    externalCount++;
            }
        }
        mapperOutput.setNoOfInternal(internalCount);
        mapperOutput.setNoOfExternal(externalCount);

        return mapperOutput;
    }

    /**
     * Extract the Partners impression details from the internal Impression details.
     * @param impression
     * @return
     * @throws MalformedURLException
     */
    public static Impression extractPartnerImpressions(Impression impression) throws MalformedURLException {
        List<InternalImpression> internalImpressions = new ArrayList<InternalImpression>();
        if(impression != null && impression.getInternal() != null){
            for(InternalImpression internalImpression : impression.getInternal()){
                if(!getDomainFromURL(internalImpression.getInternalURL()).equals(impression.getSourceDomain())){
                    internalImpression.setEventType(EventType.ExternalImpression);
                }
                internalImpressions.add(internalImpression);
            }
            impression.setInternal(internalImpressions);
        }
        return impression;
    }

    /**
     * Utils for extracting the host name from the given URL.
     * @param url
     * @return
     * @throws MalformedURLException
     */
    public static String getDomainFromURL(String url) throws MalformedURLException {
        try{
            String decodedURL = URLDecoder.decode(url, "UTF-8");
            return new URL(decodedURL).getHost().toLowerCase();
        }catch(Exception e){
            return "Invalid URL " +e.getMessage();
        }
    }

    /**
     * Generate MD5 for the given input String.
     * @param text
     * @return
     * @throws NoSuchAlgorithmException
     */
    public static String generateMD5(String text) throws NoSuchAlgorithmException {
        MessageDigest m= MessageDigest.getInstance("MD5");
        m.update(text.getBytes(), 0, text.length());
        return new BigInteger(1,m.digest()).toString(16);
    }

    /**
     * Normalize the domain, by removing www. prefix, removes starting and trailling spaces, convert the domain name to
     * lower case.
     * @param inputDomain
     * @return
     */
    public static String normalizeDomain(String inputDomain){
        if(inputDomain != null){
            inputDomain = inputDomain.trim();
            return inputDomain.replaceAll("^www[0-9]*\\.","").toLowerCase();
        }
        return "#";
    }

    /**
     * Return the local path of the Cached file based on the input filePattern
     */
    public static String getLocalCachedFile(Path[] paths, String filePattern){
        for(Path path : paths){
            if(path.toString().contains(filePattern))
                return path.toString();
        }
        return "";
    }
/*

    public static void main(String args[]) throws NoSuchAlgorithmException {
        System.out.println(generateHiveDate(1384238399976L));
    }

*/
}