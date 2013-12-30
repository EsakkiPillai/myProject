package com.ask.nrelate.fraudelent;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * User: Kaniyarasu.D
 * Date: 25/7/13
 * Time: 7:19 PM
 */

@Deprecated
public class FraudulentIPDetectionValue implements Writable{

    private String plugin;
    private Long timeInMillis;

    public FraudulentIPDetectionValue() {
    }

    public FraudulentIPDetectionValue(String plugin, Long timeInMillis) {
        this.plugin = plugin;
        this.timeInMillis = timeInMillis;
    }

    @Override
    public String toString() {
        return timeInMillis + "\t" + plugin;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(plugin);
        dataOutput.writeLong(timeInMillis);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        plugin = dataInput.readUTF();
        timeInMillis = dataInput.readLong();
    }

    @Override
    public int hashCode() {
        int result = (timeInMillis != null ? timeInMillis.hashCode() : 0);
        result = 31 * result + (plugin != null ? plugin.hashCode() : 0);
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        FraudulentIPDetectionValue fraudulentIPDetectionValue = (FraudulentIPDetectionValue) obj;
        if(fraudulentIPDetectionValue != null){
            if(this.getTimeInMillis() != fraudulentIPDetectionValue.getTimeInMillis())
                return false;
            else if(!this.getPlugin().equals(fraudulentIPDetectionValue.getPlugin()))
                return false;
        }
        return true;
    }

    public String getPlugin() {
        return plugin;
    }

    public void setPlugin(String plugin) {
        this.plugin = plugin;
    }

    public Long getTimeInMillis() {
        return timeInMillis;
    }

    public void setTimeInMillis(Long timeInMillis) {
        this.timeInMillis = timeInMillis;
    }
}
