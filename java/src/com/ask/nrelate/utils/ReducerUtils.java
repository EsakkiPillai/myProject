package com.ask.nrelate.utils;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 * User: kaniyarasu
 * Date: 15/3/13
 * Time: 4:12 PM
 * To change this template use File | Settings | File Templates.
 */
public class ReducerUtils {
    public static Map<String, String> loadClickFrequency(Reducer.Context context, Path[] paths){
        Map<String, String> clickFrequency = new HashMap<String, String>();
        BufferedReader bufferedReader;
        String lineData;

        for (Path path : paths) {
            if (path.toString().contains("suspectFrequency")) {
                try {
                    FileSystem fileSystem = FileSystem.get(context.getConfiguration());
                    bufferedReader = new BufferedReader(
                            new InputStreamReader(
                                    fileSystem.open(new Path(path.toString()))
                            ));
                    while ((lineData = bufferedReader.readLine()) != null) {
                        String[] splitData = lineData.split("=", 2);

                        if (splitData.length == 2) {
                            clickFrequency.put(splitData[0], splitData[1]);
                        }
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        return clickFrequency;
    }

    public static Map<String, String> loadClickFrequency(Path[] paths){
        Map<String, String> clickFrequency = new HashMap<String, String>(500);
        BufferedReader bufferedReader;
        String lineData;

        for(Path path : paths){
            if(path.toString().contains("suspectFrequency.dat")){
                try {
                    bufferedReader = new BufferedReader(new FileReader(path.toString()));
                    while((lineData = bufferedReader.readLine()) != null){
                            clickFrequency.put(lineData.substring(0, lineData.lastIndexOf('=')), lineData.substring(lineData.lastIndexOf('=')+1));
                    }
                    bufferedReader.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        return clickFrequency;
    }
}
