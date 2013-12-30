package com.ask.nrelate.sample;

import com.ask.nrelate.rt.pojo.Impression;
import com.ask.nrelate.rt.pojo.RTValidation;
import com.google.gson.Gson;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;

/**
 * User: Kaniyarasu
 * Date: 22/1/13
 * Time: 4:52 PM
 */
public class ImpressionTest {
    public static void main(String[] args){
        Gson gson = new Gson();
        BufferedReader  br = null;

        try {
             br = new BufferedReader(new FileReader("/home/kaniyarasu/Desktop/Imp_RT_Log"));
        } catch (FileNotFoundException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }

        //convert the json string back to object
        RTValidation obj = gson.fromJson(br, RTValidation.class);

        System.out.print(obj.getType());

        try {
            br = new BufferedReader(new FileReader("/home/kaniyarasu/Desktop/Ad_RT_Log"));
        } catch (FileNotFoundException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }

        //convert the json string back to object
        obj = gson.fromJson(br, RTValidation.class);

        System.out.print(obj.getType());

        try {
            br = new BufferedReader(new FileReader("/home/kaniyarasu/Desktop/Int_RT_Log"));
        } catch (FileNotFoundException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }

        //convert the json string back to object
        obj = gson.fromJson(br, RTValidation.class);

        System.out.print(obj.getType());
    }
}
