package com.ask.nrelate.mr.ron;

import com.ask.nrelate.pojo.LogAttributes;
import com.ask.nrelate.rt.utils.RTMapperUtils;
import com.ask.nrelate.utils.MapperUtils;

import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: kaniyarasu
 * Date: 18/3/13
 * Time: 6:51 PM
 * To change this template use File | Settings | File Templates.
 */
public class MapperUtilsTest {
    public static void main(String[] args) throws MalformedURLException {
       /* List<String> logParameterList = new ArrayList<String>(){{
            add("remoteHost");
            add("identUser");
            add("authUser");
            add("date");
            add("time");
            add("timezone");
            add("method");
            add("url");
            add("protocol");
            add("status");
            add("bytes");
            add("referrer");
            add("userAgent");
            add("remoteIP");
            add("serverIP");
        }};
        //LogAttributes logAttributes = MapperUtils.processLog("172.24.8.79, 193.5.173.214 - - [14/Jan/2013:23:46:43 -0800] \"GET /rcw_wp/0.51.1/?tag=nrelate_related&keywords=Comment+retrouver+le+code+IMEI+de+son+t%C3%A9l%C3%A9phone+portable+%3F&domain=www.toile-filante.com&url=http%3A%2F%2Fwww.toile-filante.com%2F2010%2F01%2F12%2Fcomment-retrouver-le-code-imei-de-son-telephone-portable%2F&nr_div_number=1 HTTP/1.1\" 200 2599 \"http://www.toile-filante.com/2010/01/12/comment-retrouver-le-code-imei-de-son-telephone-portable/\" \"Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 5.1; Trident/4.0; .NET CLR 1.1.4322; .NET CLR 2.0.50727; .NET CLR 3.5.30729; InfoPath.2; .NET4.0C; .NET4.0E; .NET CLR 3.0.4506.2152)\"", logParameterList);
        String lineData = "72.74.48.159Mozilla/5.0 (Macintosh; Intel Mac OS X 10.5; rv:16.0) Gecko/20100101 Firefox/16.0type:ad=1358247599";
        System.out.println(lineData);  //Arrays.
        System.out.println(lineData.substring(0, lineData.lastIndexOf('=')));
        System.out.println(lineData.substring(lineData.lastIndexOf('=')+1));
*/
        String url ="http%3a%2f%2fcelebrityhollywoodgossip.com%2fk-michelles-camel-toe-catsuit-c";
        url="http://celebrityhollywoodgossip.com/k-michelles-camel-toe-catsuit-c";
        try {
            String decodedURL = URLDecoder.decode(url,"UTF-8");

            System.out.print(decodedURL+"\n"+RTMapperUtils.getDomainFromURL(URLDecoder.decode(decodedURL,"UTF-8")));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }
}
