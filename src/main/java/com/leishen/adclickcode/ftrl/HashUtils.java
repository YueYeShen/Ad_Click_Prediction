package com.leishen.adclickcode.ftrl;


import java.util.ArrayList;
import java.util.List;

public class HashUtils {
    public static int hash(String data) {
        int t = 0;
        int length = data.length();
        //System.out.println(length);
        int zeroAscll = data.charAt(0);
        int leftSeven = zeroAscll << 7;
        //System.out.println(leftSeven);
        for (int i = 0; i < length; i++) {
            int y = data.charAt(i);
            //System.out.println(y);
            leftSeven = (leftSeven * 13) ^ y;
            //System.out.println("this is leftSeven "+ leftSeven);
        }
        leftSeven = leftSeven ^ length;
        return leftSeven;
    }



    public static List<Integer> hashDatas(String data , long hashSize, String halt){

        List<Integer> dataIndexs = new ArrayList<Integer>();

        String [] allFeature= data.trim().split(";");
        FM_FTRL_Parameter_Helper parameter_Helper = new FM_FTRL_Parameter_Helper();
        for(String oneData : allFeature){
            int  valueIndex =Math.abs(hash(oneData) )  % parameter_Helper.getHashSize()+1;
           // System.out.println("this is datahelper hash data  "+valueIndex);
            dataIndexs.add(valueIndex);
        }

        return dataIndexs;


    }
}
