package com.ask.nrelate.cassa;
import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.NoHostAvailableException;


import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
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
public class CassandraDataLoadWithCollection {
    static String KEYSPACE = "nrelate";
    static String tableName;
    static Session session;
    private Logger logger = Logger.getLogger(CassandraDataLoadWithCollection.class.getName()) ;


    public void initialize(String tableName,String[] seeds) {
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
    public void insert(String filename) throws IOException {
        PreparedStatement ps = session.prepare("INSERT INTO "+tableName+" (user_id, first_name,last_name,emails) VALUES (?,?,?,?)");
        BatchStatement batch = new BatchStatement();
        BufferedReader reader = null;
        Statement statement = null;
        //batch.setConsistencyLevel(ConsistencyLevel.ONE);
        String line;
        try{
            reader = new BufferedReader(new FileReader(filename));
        }catch (Exception e){
            logger.info("Input file not found");
            logger.exiting(CassandraDataLoadWithCollection.class.getName(),"insert method");
        }
        int lineNumber = 1;
        //try{
            while ((line = reader.readLine()) != null) {
                SimpleProperties cassandraProperties = new SimpleProperties();
                if (parse(cassandraProperties,line, lineNumber++)) {
                    try{
                        batch = new BatchStatement();
                        //statement = ps.bind(cassandraProperties.getKey(), cassandraProperties.getPixeldate(), cassandraProperties.getImpressions(), cassandraProperties.getPaidimpressions(),cassandraProperties.getAds(), cassandraProperties.getInternals(), cassandraProperties.getExternals(),cassandraProperties.getGrossRevenue(),
                          //      cassandraProperties.getNetRevenue(), cassandraProperties.getGrossRPM(), cassandraProperties.getNetRPM(), cassandraProperties.getAdCTR(), cassandraProperties.getInternalCTR());
                        statement = ps.bind(cassandraProperties.getUserId(),cassandraProperties.getFirstName(),cassandraProperties.getLastName(),cassandraProperties.getEmails());
                        long start = System.currentTimeMillis();
                        batch.add(statement);
                        session.execute(batch);
                    }catch (Exception e){
                        System.out.println(e.getMessage());
                    }  finally {
                        session.execute(batch);
                    }
                }
            }
        //}catch (Exception e){
        //    logger.info("EXCEPTION: while reading the file content"+e.getMessage());
        //}
    }

    boolean parse(SimpleProperties cassandraProperties,String line, int lineNumber) {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-M-dd");

        String[] values = line.split("\\,");
        if (values.length != 5) {
            System.out.println(values.length + " " + String.format("Invalid input '%s' at line %d", line, lineNumber));
            return false;
        }
        try {
            int i=0;
            Set<String> emails = new HashSet<String>();
            cassandraProperties.setUserId(values[i++].trim());
            cassandraProperties.setFirstName(values[i++]);
            cassandraProperties.setLastName(values[i++]);
            //cassandraProperties.setEmails(values[i++]);
            //cassandraProperties.setEmails(values[i++]);
            emails.add(values[i++]);emails.add(values[i++]);
            cassandraProperties.setEmails(emails);

            return true;
        } catch (NumberFormatException e) {
            System.out.println(String.format("Invalid number in input '%s' at line %d", line, lineNumber));
            return false;
        }
    }
    public void shutdown() {
        if (session != null) {
            System.out.println("Shutting down the session");
            session.shutdown();

        }
    }
    public static void main(String args[]) throws IOException {
        if(args.length!=3){
            System.out.println("Usage: java CassandraDataLoad TableName SeedsSeperatedByComma InputFilePathSeparatedByTab");
            System.out.println("Example: java CassandraDataLoad nrelate_data 10.168.6.95,10.31.137.47 /home/nrelate/nrelate_data.tsv");
            System.out.println("Exiting the program");
            System.exit(1);
        }
        String tableName = args[0];
        String seeds[] = args[1].split("\\,");
        String filename = args[2];
        System.out.println("Filename is "+filename);
        CassandraDataLoadWithCollection cassandraDataLoader = new CassandraDataLoadWithCollection();
        cassandraDataLoader.initialize(tableName,seeds);
        cassandraDataLoader.insert(filename);
        cassandraDataLoader.shutdown();
        System.out.println("Process over");
        System.exit(0);
    }

}
