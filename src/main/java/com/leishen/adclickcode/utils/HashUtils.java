package com.leishen.adclickcode.utils;


import com.leishen.adclickcode.ftrl.FM_FTRL_Parameter_Helper;

import java.util.ArrayList;
import java.util.List;

public class HashUtils {
    public static int hash(String data) {

        int length = data.length();
        int zeroAscll = data.charAt(0);
        int leftSeven = zeroAscll << 7;
        for (int i = 0; i < length; i++) {
            int y = data.charAt(i);

            leftSeven = (leftSeven * 13) ^ y;

        }
        leftSeven = leftSeven ^ length;
        return leftSeven;
    }


    public static List<Integer> hashDatas(String data, long hashSize, String halt) {

        List<Integer> dataIndexs = new ArrayList<Integer>();

        String[] allFeature = data.trim().split(";");
        FM_FTRL_Parameter_Helper parameter_Helper = new FM_FTRL_Parameter_Helper();
        for (String oneData : allFeature) {
            int valueIndex = Math.abs(hash(oneData)) % parameter_Helper.getHashSize() + 1;

            dataIndexs.add(valueIndex);
        }

        return dataIndexs;


    }
}