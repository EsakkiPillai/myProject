package com.ask.nrelate.rt.pojo;

import com.ask.nrelate.rt.utils.json.Required;
import com.ask.nrelate.rt.utils.json.ValidationType;
import com.google.gson.annotations.SerializedName;

/**
 * Created by IntelliJ IDEA.
 * User: kaniyarasu
 * Date: 8/11/13
 * Time: 10:42 AM
 * To change this template use File | Settings | File Templates.
 */
public class AdImpression {

    @Required(fieldName = "adurl", fieldType = "java.lang.String", validationType = ValidationType.REQ)
    @SerializedName("adurl")
    private String adURL="#";

    @Required(fieldName = "cid", fieldType = "java.lang.Double", validationType = ValidationType.ALL)
    private double cid=0;

    @Required(fieldName = "cpc", fieldType = "java.lang.Double", validationType = ValidationType.ALL)
    private double cpc=0;

    @SerializedName("OPEDID")
    private String opedid="#";

    @SerializedName("img_maturity_level")
    private String imgMaturityLevel="#";

    //@SerializedName("txt_maturity_level")
    private String txtMaturityLevel="#";

    private int maturity=0;

    public String getImgMaturityLevel() {
        return imgMaturityLevel;
    }

    public void setImgMaturityLevel(String imgMaturityLevel) {
        this.imgMaturityLevel = imgMaturityLevel;
    }

    public int getMaturity() {
        return maturity;
    }

    public void setMaturity(int maturity) {
        this.maturity = maturity;
    }

    public String getTxtMaturityLevel() {
        return txtMaturityLevel;
    }

    public void setTxtMaturityLevel(String txtMaturityLevel) {
        this.txtMaturityLevel = txtMaturityLevel;
    }

    public String getAdURL() {
        return adURL;
    }

    public void setAdURL(String adURL) {
        this.adURL = adURL;
    }

    public double getCid() {
        return cid;
    }

    public void setCid(double cid) {
        this.cid = cid;
    }

    public double getCpc() {
        return cpc;
    }

    public void setCpc(double cpc) {
        this.cpc = cpc;
    }

    public String getOpedid() {
        return opedid;
    }

    public void setOpedid(String opedid) {
        this.opedid = opedid;
    }
}