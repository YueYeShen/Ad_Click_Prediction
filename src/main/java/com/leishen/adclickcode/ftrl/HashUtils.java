package com.leishen.adclickcode.ftrl;



public class HashUtils {
public static int hash(String data){
	int t = 0;
	int length = data.length();
	//System.out.println(length);
	int zeroAscll = data.charAt(0);
	int leftSeven = zeroAscll << 7;
	//System.out.println(leftSeven);
	for(int i =0;i<length;i++){
		int y = data.charAt(i);
		//System.out.println(y);
		leftSeven = (leftSeven *13)^y;
		//System.out.println("this is leftSeven "+ leftSeven);
	}
	leftSeven = leftSeven ^ length;
	return leftSeven;
}
}
