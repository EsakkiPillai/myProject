package com.ask.nrelate.sample;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.GregorianCalendar;

/**
 * User: kaniyarasu
 * Date: 22/1/13
 * Time: 3:43 PM
 * To change this template use File | Settings | File Templates.
 */
public class JSONTest {
   public static void main(String[] args) {

       // Gson gson = new Gson();
       SimpleDateFormat hiveTimeFormat = new SimpleDateFormat("HH:mm:ss");
       Calendar calendar = GregorianCalendar.getInstance();
        try {
            //System.out.print(String.valueOf(hiveTimeFormat.parse("01:30:31").getTime() / (1000)));

            calendar.setTime(hiveTimeFormat.parse("00:29:59"));
            System.out.println(calendar.get(Calendar.HOUR_OF_DAY) * 2 + calendar.get(Calendar.MINUTE) / 30 + 1);
            System.out.println(Math.abs(calendar.getTimeInMillis())/1000);
            System.out.println("3232323\trc".substring("3232323\trc".indexOf("\t")).trim());
            System.out.println("3232323\trc".substring(0, "3232323\trc".indexOf("\t")).trim());
/*
            String r1 = "^(\\S+)\\t([^\"]+)\\t(\\S+)\\t(\\S+)\\t(\\S+)\\t(\\S+)$";
            Pattern pattern = Pattern.compile(r1);
            Matcher matcher = pattern.matcher("66.249.82.89\tMozilla/5.0 (Linux; Android 4.0.4; Galaxy Nexus Build/IMM76B) AppleWebKit/537.4 (KHTML, like Gecko) Chrome/22.0.1229 Mobile Safari/537.4 pss-webkit-request\tbasketball-wallpapers.net\trc\timpression\t00:00:37");
            //System.out.print(matcher.groupCount());
            while(matcher.find()){
                for(int i=1;i<=matcher.groupCount();i++){
                    System.out.println(matcher.group(i));
                }
            }*/

            /*List internal = obj.getInternal();
            for(int i=0; i<internal.size(); i++){
                //Long count = hTable.incrementColumnValue(Bytes.toBytes(hbaseKey), Bytes.toBytes("daily"), Bytes.toBytes(qualifier), 1L, false);
            }
            System.out.println(obj);
            com.ask.nrelate.rt.pojo.Click obj1 = gson.fromJson(br1, com.ask.nrelate.rt.pojo.Click.class);
            System.out.println(obj1);*/

        } catch (Exception e) {
            e.printStackTrace();
        }

    }


}
