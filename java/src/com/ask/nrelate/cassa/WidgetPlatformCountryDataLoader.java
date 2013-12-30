package com.ask.nrelate.cassa;
import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.NoHostAvailableException;

import java.io.BufferedReader;
import java.io.FileReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Logger;

/**
 * Created with IntelliJ IDEA.
 * User: sivakumar
 * Date: 26/11/13
 * Time: 7:28 PM
 * To change this template use File | Settings | File Templates.
 */
public class WidgetPlatformCountryDataLoader {
    static String KEYSPACE = "nrelate";
    static String tableName;
    static Session session;
    static String operation;
    private Logger logger = Logger.getLogger(WidgetPlatformCountryDataLoader.class.getName()) ;



    public void initialize(String tableName,String[] seeds,String operation) {
        this.operation = operation;
        this.tableName = tableName ;
        try {
            if (session == null) {
                Cluster.Builder builder = Cluster.builder();
                for (String seed : seeds) {
                    builder.addContactPoint(seed);
                }
                Cluster cluster = builder.build();
                session = cluster.connect(KEYSPACE);
            }
        } catch (NoHostAvailableException e) {
            throw new RuntimeException(e);
        }
    }
    public void insert(String filename) {
        PreparedStatement ps = session.prepare("INSERT INTO "+tableName+" (domain,pixeldate,id,widgetid,platform,country,impressions,paidimpressions,ads,internals,externals,gross_revenue,net_revenue,gross_rpm,net_rpm,ad_ctr,internal_ctr) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
        PreparedStatement deleteStatement = session.prepare("delete from "+tableName+" where domain=? and pixeldate=?");
        BatchStatement batch = new BatchStatement();
        batch.setConsistencyLevel(ConsistencyLevel.TWO);
        BatchStatement deleteBatch = new BatchStatement();
        deleteBatch.setConsistencyLevel(ConsistencyLevel.TWO);
        BufferedReader reader = null;
        Statement statement = null;
        int deleteCount = 0;
        Set<String> domainsSet = new HashSet<String>();

        String line;
        try{
            reader = new BufferedReader(new FileReader(filename));
        }catch (Exception e){
            logger.info("Input file not found");
            logger.exiting(WidgetPlatformCountryDataLoader.class.getName(),"insert method");
        }
        if(operation.equals("delete")){
            try{
                while ((line = reader.readLine()) != null) {
                    String tmpDomain = line.split("\\t")[0].trim();
                    SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-M-dd");
                    Date date = simpleDateFormat.parse(line.split("\\t")[1].trim());
                    //domainsSet.add(tmpDomain);
                    if(!domainsSet.contains(tmpDomain)){
                        //String query = "delete from "+tableName+" where domain='"+tmpDomain+"' and pixeldate='2013-12-09'";
                        statement = deleteStatement.bind(tmpDomain,date);
                        //session.execute(statement);
                        deleteBatch.add(statement);
                        domainsSet.add(tmpDomain);
                        System.out.println("DELETING:"+tmpDomain+" DomainNo:"+ ++deleteCount);
                        if(deleteCount%75==0){
                            session.execute(statement);
                            deleteBatch = new BatchStatement();
                        }
                    }
                }
                session.execute(deleteBatch);
            }catch (Exception e){
                System.out.println(e.getMessage());
            }
        /*String domains = "";
        for(String domain:domainsSet)   {
            domains = domains+"'"+domain+"'"+",";
        }
        domains = domains.substring(0,domains.length()-1);
        System.out.println(domains);
        String query = "delete from "+tableName+" where domain in ("+domains+") and pixeldate='2013-12-09'";
        session.execute(query);*/
        }else if(operation.equals("insert")){
            long lineNumber = 0;
            try{
                while ((line = reader.readLine()) != null) {
                    nRelateBean bean = new nRelateBean();
                    if (parse(bean,line, lineNumber++)) {
                        try{
                            statement = ps.bind(bean.getDomain(), bean.getPixeldate(),lineNumber,bean.getWidgetid(),bean.getPlatform(), bean.getCountry(),bean.getImpressions(), bean.getPaidimpressions(),bean.getAds(), bean.getInternals(), bean.getExternals(),bean.getGrossRevenue(),
                                    bean.getNetRevenue(), bean.getGrossRPM(), bean.getNetRPM(), bean.getAdCTR(), bean.getInternalCTR());
                            long start = System.currentTimeMillis();
                            batch.add(statement);

                            /*if(!domainsSet.contains(bean.getDomain())){
                                String query = "delete from "+tableName+" where domain='"+bean.getDomain()+"' and pixeldate='2013-12-09'";
                                domainsSet.add(bean.getDomain());
                                session.execute(query);
                                System.out.println("DELETING:"+bean.getDomain()+" DomainNo:"+ ++deleteCount);
                            }*/
                            //statement = deleteStatement.bind(bean.getDomain(),bean.getPixeldate());
                            //deleteBatch.add(statement);
                            //session.execute(statement);
                            System.out.println("line number:"+lineNumber+" date:"+bean.getPixeldate()+" domain:"+bean.getDomain());
                            if(lineNumber%75==0){
                                //session.execute(deleteBatch);
                                //deleteBatch = new BatchStatement();
                                session.execute(batch);
                                batch = new BatchStatement();
                            }
                        }catch (Exception e){
                            System.out.println(e.getMessage());
                        }
                    }
                }
            }catch (Exception e){
                logger.info("EXCEPTION: while reading the file content"+e.getMessage());
            }
            //session.execute(deleteBatch);
            session.execute(batch);

        }else{
            System.out.println("Invalid operation value. Valid are insert or delete");
        }
    }

    boolean parse(nRelateBean bean,String line, long lineNumber) {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-M-dd");

        String[] values = line.split("\\t");
        if (values.length != 16) {
            System.out.println(values.length + " " + String.format("Invalid input '%s' at line %d", line, lineNumber));
            return false;
        }
        try {
            int i=0;
            bean.setDomain(values[i++].trim());
            Date pixeldate = simpleDateFormat.parse(values[i++].trim());
            bean.setPixeldate(pixeldate);
            if(tableName.equals("nrelate_data_hourly")){
                bean.setPixelhour(Integer.parseInt(values[i++].trim()));
            }
            bean.setWidgetid(Long.parseLong(values[i++].trim()));
            bean.setPlatform(values[i++].trim());
            bean.setCountry(values[i++].trim());
            bean.setImpressions(Long.parseLong(values[i++].trim()));
            bean.setPaidimpressions(Long.parseLong(values[i++].trim()));
            bean.setAds(Long.parseLong(values[i++].trim()));
            bean.setInternals(Long.parseLong(values[i++].trim()));
            bean.setExternals(Long.parseLong(values[i++].trim()));
            bean.setGrossRevenue(Double.parseDouble(values[i++].trim()));
            bean.setNetRevenue(Double.parseDouble(values[i++].trim()));
            bean.setGrossRPM(Float.parseFloat(values[i++].trim()));
            bean.setNetRPM(Float.parseFloat(values[i++].trim()));
            bean.setAdCTR(Float.parseFloat(values[i++].trim()));
            bean.setInternalCTR(Float.parseFloat(values[i++].trim()));

            return true;
        } catch (NumberFormatException e) {
            System.out.println(String.format("Invalid number in input '%s' at line %d", line, lineNumber));
            return false;
        } catch (ParseException e){
            System.out.println("Exception while parsing the date "+e.getMessage());
            return false;
        }
    }
    public void shutdown() {
        if (session != null) {
            System.out.println("Shutting down the session");
            session.shutdown();

        }
    }
    public static void main(String args[]) {
        if(args.length!=4){
            System.out.println("Usage: java WidgetPlatformCountryDataLoader TableName SeedsSeperatedByComma InputFilePathSeparatedByTab operataion");
            System.out.println("Example: java WidgetPlatformCountryDataLoader nrelate_data 10.168.6.95,10.31.137.47 /home/nrelate/nrelate_data.tsv insert/delete");
            System.out.println("Exiting the program");
            System.exit(1);
        }
        String tableName = args[0];
        String seeds[] = args[1].split("\\,");
        String filename = args[2];
        String dmlOperation = args[3];
        WidgetPlatformCountryDataLoader widgetDataLoader = new WidgetPlatformCountryDataLoader();
        widgetDataLoader.initialize(tableName,seeds,dmlOperation);
        widgetDataLoader.insert(filename);
        widgetDataLoader.shutdown();
        System.out.println("Process over");
        System.exit(0);
    }

}
