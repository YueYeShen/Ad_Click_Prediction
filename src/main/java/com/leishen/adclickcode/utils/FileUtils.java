package com.leishen.adclickcode.utils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

/**
 * Created by leishen on 2016/12/12 0012.
 */
public class FileUtils {
    public static void writeContent(String filePath, String content) {
        File file = new File(filePath);
        FileWriter fileWritter;
        try {
            fileWritter = new FileWriter(file, true);
            BufferedWriter bufferWritter = new BufferedWriter(fileWritter);
            bufferWritter.write(content);
            bufferWritter.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public static void createFile(File file) {
        if (!file.exists())
            try {
                file.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
    }

    public static  void createFile(String filePath) {
        File newFile = new File(filePath);
        if (!newFile.exists()) {
            try {
                newFile.createNewFile();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }
}
