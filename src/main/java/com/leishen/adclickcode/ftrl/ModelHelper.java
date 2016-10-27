package com.leishen.adclickcode.ftrl;

import java.util.List;

public class ModelHelper {
	// 训练模型的名字
	String name = "";
	// 训练数据组成的集合
	List<String> dataList = null;
	// 模型的成熟度
	float mature = 0;
	// 模型训练器
	FM_FTRL_machine learner = null;

	// 初始化函数
	public ModelHelper(String name, List<String> dataList, float mature) {
		this.name = name;
		this.dataList = dataList;
		this.mature = mature;
		// 定义零时参数类
		FM_Parameter_Helper parameter_Helper = new FM_Parameter_Helper();
		// 初始化学习器，并传入模型的名字和相应参数的列表
		learner = new FM_FTRL_machine(name, parameter_Helper.getparameterList());

	}

	public void startTrainWork() {
		this.learner.initUseFilePath();
		
		if (this.mature != 0) {
			//加载上次训练完之后的参数
			this.learner.loadFMAndWParameters();
		}
		

	}

}
