package com.ask.nrelate.fraudelent;

import com.ask.nrelate.utils.StringConstants;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * User: Kaniyarasu.D
 * Date: 25/7/13
 * Time: 1:12 PM
 */
public class FraudulentIPDetectionKeyWODomain implements WritableComparable<FraudulentIPDetectionKeyWODomain>{
    private String ipAddress;
    private String userAgent;
    private String requestType;

    public FraudulentIPDetectionKeyWODomain() {
        super();
    }

    public FraudulentIPDetectionKeyWODomain(String ipAddress, String userAgent, String requestType) {
        this.ipAddress = ipAddress;
        this.userAgent = userAgent;
        this.requestType = requestType;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(ipAddress);
        dataOutput.writeUTF(userAgent);
        dataOutput.writeUTF(requestType);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        ipAddress = dataInput.readUTF();
        userAgent = dataInput.readUTF();
        requestType = dataInput.readUTF();
    }

    @Override
    public int compareTo(FraudulentIPDetectionKeyWODomain fraudulentIPDetectionKey) {
        if(fraudulentIPDetectionKey != null){
            int result = ipAddress.compareTo(fraudulentIPDetectionKey.getIpAddress());
            if(result == 0 && userAgent != null && fraudulentIPDetectionKey.getUserAgent() != null)
                result = userAgent.compareTo(fraudulentIPDetectionKey.getUserAgent());
            if(result == 0 && requestType != null && fraudulentIPDetectionKey.getRequestType() != null)
                result = requestType.compareTo(fraudulentIPDetectionKey.getRequestType());

            return result;
        }
        return 0;
    }

    @Override
    public boolean equals(Object obj) {
        FraudulentIPDetectionKeyWODomain fraudulentIPDetectionKey = (FraudulentIPDetectionKeyWODomain) obj;
        if(obj != null){
            if(!this.getIpAddress().equals(fraudulentIPDetectionKey.getIpAddress()))
                return false;
            else if(!this.getUserAgent().equals(fraudulentIPDetectionKey.getUserAgent()))
                return false;
            else if(!this.getRequestType().equals(fraudulentIPDetectionKey.getRequestType()))
                return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return ipAddress + StringConstants.tab
                + userAgent + StringConstants.tab
                + requestType;
    }

    @Override
    public int hashCode() {
        //int result = (timeSlot != null ? timeSlot.hashCode() : 0);
        int result = 0;
        result = 31 * result + (ipAddress != null ? ipAddress.hashCode() : 0);
        result = 31 * result + (userAgent != null ? userAgent.hashCode() : 0);
        result = 31 * result + (requestType != null ? requestType.hashCode() : 0);
        return result;
    }

    public String getIpAddress() {
        return ipAddress;
    }

    public void setIpAddress(String ipAddress) {
        this.ipAddress = ipAddress;
    }

    public String getUserAgent() {
        return userAgent;
    }

    public void setUserAgent(String userAgent) {
        this.userAgent = userAgent;
    }

    public String getRequestType() {
        return requestType;
    }

    public void setRequestType(String requestType) {
        this.requestType = requestType;
    }
}
