package com.leishen.adclickcode.ftrl;

import java.util.ArrayList;
import java.util.List;

public class FM_FTRL_Parameter_Helper {
	 int fm_dim = 4; 
	 float fm_initDev;
	 String hashSalt = "salty"; //hash的时候加上这个来hash
	 float alpha;               //学习的速率，类似于梯度下降的步长
	 float beta;

	 float alpha_fm;
	 float beta_fm;

	 int p_D = 22;
	 int D;

	 float L1;
	 float L2;
	 float L1_fm;
	 float L2_fm;
	 float dropoutRate;
	 int n = 5;                //更新的时候，每次用5个循环来更新
	public int getFm_dim() {
		return fm_dim;
	}

	public void setFm_dim(int fm_dim) {
		this.fm_dim = fm_dim;
	}

	public float getFm_initDev() {
		return fm_initDev;
	}

	public void setFm_initDev(float fm_initDev) {
		this.fm_initDev = fm_initDev;
	}

	public String getHashSalt() {
		return hashSalt;
	}

	public void setHashSalt(String hashSalt) {
		this.hashSalt = hashSalt;
	}

	public float getAlpha() {
		return alpha;
	}

	public void setAlpha(float alpha) {
		this.alpha = alpha;
	}

	public float getBeta() {
		return beta;
	}

	public void setBeta(float beta) {
		this.beta = beta;
	}

	public float getAlpha_fm() {
		return alpha_fm;
	}

	public void setAlpha_fm(float alpha_fm) {
		this.alpha_fm = alpha_fm;
	}

	public float getBeta_fm() {
		return beta_fm;
	}

	public void setBeta_fm(float beta_fm) {
		this.beta_fm = beta_fm;
	}

	public int getHashSize() {
		return D;
	}

	public void setHashSize(int d) {
		D = d;
	}

	public float getL1() {
		return L1;
	}

	public void setL1(float l1) {
		L1 = l1;
	}

	public float getL2() {
		return L2;
	}

	public void setL2(float l2) {
		L2 = l2;
	}

	public float getL1_fm() {
		return L1_fm;
	}

	public void setL1_fm(float l1_fm) {
		L1_fm = l1_fm;
	}

	public float getL2_fm() {
		return L2_fm;
	}

	public void setL2_fm(float l2_fm) {
		L2_fm = l2_fm;
	}

	public float getDropoutRate() {
		return dropoutRate;
	}

	public void setDropoutRate(float dropoutRate) {
		this.dropoutRate = dropoutRate;
	}

	public int getN() {
		return n;
	}

	public void setN(int n) {
		this.n = n;
	}

	
	
	//初始化每个参数的值，可以先使用下面的函数修改
	public FM_FTRL_Parameter_Helper(){
		fm_dim = 4;
		fm_initDev = 0.01f;
		String hashSalt = "salty";
		alpha = 0.2f;
		beta = 1.0f;

		alpha_fm = 0.1f;
		beta_fm = 1.0f;

		int p_D = 20;
		D = (int)Math.pow(2, p_D);

		L1 = 1.0f;
		L2 = 0.1f;
		L1_fm = 2.0f;
		L2_fm = 3.0f;
		dropoutRate = 1.0f;
		int n = 5;
		
	}
	
	//将每个参数装进集合中
	public  List<Object> getParameterList(){
		List<Object> parameterList = new ArrayList<Object>();
		parameterList.add(alpha);
        parameterList.add(beta);
        parameterList.add(L1);
        parameterList.add(L2);
        parameterList.add(alpha_fm);
        parameterList.add(beta_fm);
        parameterList.add(L1_fm);
        parameterList.add(L2_fm);
        parameterList.add(fm_dim);
        parameterList.add(fm_initDev);
        parameterList.add(dropoutRate);
        parameterList.add(D);
        parameterList.add(n);
        return parameterList;
	}

}
