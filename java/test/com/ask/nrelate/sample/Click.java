package com.ask.nrelate.sample;

/**
 * User: kaniyarasu
 * Date: 22/1/13
 * Time: 5:24 PM
 */
public class Click {
    private String dom = "";
    private String src_url = "";
    private String dst_url = "";
    private String type = "";
    private String cid = "NA";
    private String plugin = "";

    @Override
    public String toString() {
        return "Click[Domain:" + dom + ",Source URL:" + src_url + ",Destination URL:" + dst_url
                + ", Type:" + type + ", CID:" + cid + ",Plugin:" + plugin + "]";
    }
}
