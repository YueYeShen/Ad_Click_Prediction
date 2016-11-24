package com.leishen.adclickcode.trainkandpredict

import com.leishen.adclickcode.ftrl.{FM_FTRL_Parameter_Helper, FM_FTRL_machine, HashUtils}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext}

/**
  * Created by leishen on 2016/11/24
  */
object PredictData {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("ModelTrain").setMaster("local")
    sparkConf.set("spark.local.dir","S:\\Data")
    val sparkContext = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sparkContext)

    val pageViewData = sqlContext.read.parquet("S:\\Kaggle Data\\page_views_join_events_joinTestAD")

    val learnerParameters = new FM_FTRL_Parameter_Helper
    val learner = new FM_FTRL_machine("Ad_Click", learnerParameters.getParameterList);

    learner.loadFMAndWParameters()

    val testData = pageViewData.rdd.collect()
    val outputMap = Map.empty[String,List[String]]


    for(row  <- testData){
      val rowString  = changeRowToString(row)
      val predictValue = learner.predict(HashUtils.hashDatas(rowString, learnerParameters.getHashSize, "kaggle"))
      if(predictValue > 0.4){

      }
    }



  }

  private def changeRowToString(row: Row): String = {
    val builder = new StringBuilder
    for (i <- 0 to row.length - 2) {
      builder.append(row(i) + ";")
    }
    builder.append(row(row.length - 1))
    builder.toString()
  }

}
