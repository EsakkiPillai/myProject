package com.ask.nrelate.cassandra;

import com.datastax.driver.core.*;
//import com.datastax.driver.core.exceptions.NoHostAvailableException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created with IntelliJ IDEA.
 * User: sivakumar
 * Date: 27/11/13
 * Time: 12:46 PM
 * To change this template use File | Settings | File Templates.
 */
public class CassandraDataLoadMapper extends Mapper<LongWritable, Text, Text, Text> implements CassandraUtils{
    static Session session;
    protected void setup(Context context) throws IOException, InterruptedException {
        try {
            if (session == null) {
                Cluster.Builder builder = Cluster.builder();
                for (String seed : seeds) {
                    builder.addContactPoint(seed);
                }
                Cluster cluster = builder.build();
                session = cluster.connect(KEYSPACE);
            }
        } catch (Exception e) {
            throw new RuntimeException(e+" during setup");
        }
    }

    public void map(LongWritable logKey, Text value, Context context) throws IOException, InterruptedException{
        Text mapperKey = new Text(String.valueOf(Math.random()*100));
        PreparedStatement ps = session.prepare("INSERT INTO "+TABLE_NAME+" (key, pixeldate,impressions,paidimpressions,ads,internals,externals,gross_revenue,net_revenue,gross_rpm,net_rpm,ad_ctr,internal_ctr) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)");
        //BatchStatement batch = new BatchStatement();
        Statement statement = null;
        CassandraProperties cassandraProperties = new CassandraProperties();
        if (parse(cassandraProperties,value.toString())) {
            try{
                //batch = new BatchStatement();
                statement = ps.bind(cassandraProperties.getKey(), cassandraProperties.getPixeldate(), cassandraProperties.getImpressions(), cassandraProperties.getPaidimpressions(),cassandraProperties.getAds(), cassandraProperties.getInternals(), cassandraProperties.getExternals(),cassandraProperties.getGrossRevenue(),
                        cassandraProperties.getNetRevenue(), cassandraProperties.getGrossRPM(), cassandraProperties.getNetRPM(), cassandraProperties.getAdCTR(), cassandraProperties.getInternalCTR());
                long start = System.currentTimeMillis();
                //batch.add(statement);
                session.execute(statement);
            }catch (Exception e){
                System.out.println(e.getMessage());
            }
        }

        context.write(mapperKey,mapperKey);
    }
    boolean parse(CassandraProperties cassandraProperties,String line) {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-M-dd");

        String[] values = line.split("\\t");
        if (values.length != 13) {
            System.out.println(values.length + " " + String.format("Invalid input '%s' at line %d", line));
            return false;
        }
        try {
            int i=0;
            cassandraProperties.setKey(values[i++].trim());
            Date pixeldate = simpleDateFormat.parse(values[i++].trim());
            cassandraProperties.setPixeldate(pixeldate);
            if(TABLE_NAME.equals("nrelate_data_hourly")){
                cassandraProperties.setPixelhour(Integer.parseInt(values[i++].trim()));
            }
            cassandraProperties.setImpressions(Long.parseLong(values[i++].trim()));
            cassandraProperties.setPaidimpressions(Long.parseLong(values[i++].trim()));
            cassandraProperties.setAds(Long.parseLong(values[i++].trim()));
            cassandraProperties.setInternals(Long.parseLong(values[i++].trim()));
            cassandraProperties.setExternals(Long.parseLong(values[i++].trim()));
            cassandraProperties.setGrossRevenue(Double.parseDouble(values[i++].trim()));
            cassandraProperties.setNetRevenue(Double.parseDouble(values[i++].trim()));
            cassandraProperties.setGrossRPM(Float.parseFloat(values[i++].trim()));
            cassandraProperties.setNetRPM(Float.parseFloat(values[i++].trim()));
            cassandraProperties.setAdCTR(Float.parseFloat(values[i++].trim()));
            cassandraProperties.setInternalCTR(Float.parseFloat(values[i++].trim()));

            return true;
        } catch (NumberFormatException e) {
            System.out.println(String.format("Invalid number in input '%s' at line %d", line));
            return false;
        } catch (ParseException e){
            System.out.println("Exception while parsing the date "+e.getMessage());
            return false;
        }
    }
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        shutdown();
    }
    public void shutdown() {
        if (session != null) {
            System.out.println("Shutting down the session");
            session.shutdown();

        }
    }
}
