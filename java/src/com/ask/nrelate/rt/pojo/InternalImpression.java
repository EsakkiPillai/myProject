package com.ask.nrelate.rt.pojo;

import com.ask.nrelate.rt.utils.EventType;
import com.google.gson.annotations.SerializedName;

/**
 * Created by IntelliJ IDEA.
 * User: kaniyarasu
 * Date: 8/11/13
 * Time: 10:41 AM
 * To change this template use File | Settings | File Templates.
 */
public  class InternalImpression {
    @SerializedName("inturl")
    private String internalURL="#";

    @SerializedName("OPEDID")
    private String opedid="#";
    
    private EventType eventType = EventType.InternalImpression;

    public EventType getEventType() {
        return eventType;
    }

    public void setEventType(EventType eventType) {
        this.eventType = eventType;
    }

    public String getInternalURL() {
        return internalURL;
    }

    public void setInternalURL(String internalURL) {
        this.internalURL = internalURL;
    }

    public String getOpedid() {
        return opedid;
    }

    public void setOpedid(String opedid) {
        this.opedid = opedid;
    }
}