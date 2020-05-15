package com.jingxin.framework.datax.enhance.core.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class FileLogUtil {

    public static String getJobRunningLog(String file, String jobId) {
        try (FileReader fileReader = new FileReader(new File(file))) {
            BufferedReader reader = new BufferedReader(fileReader);
            List<String> list = new ArrayList<String>();
            String line = reader.readLine();
            while (line != null) {
                if (line.startsWith(jobId)){
                    list.add(line);
                }
                line = reader.readLine();
            }
            return list.stream().collect(Collectors.joining("\n"));
        } catch(FileNotFoundException e){
            e.printStackTrace();
        } catch(IOException e){
            e.printStackTrace();
        }
        return "";
    }
}
