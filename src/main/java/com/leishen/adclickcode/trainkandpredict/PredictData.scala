package com.leishen.adclickcode.trainkandpredict

import java.util

import com.leishen.adclickcode.ftrl.{FM_FTRL_Machine, FM_FTRL_Parameter_Helper}
import com.leishen.adclickcode.utils.HashUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext}

/**
  * Created by leishen on 2016/11/24
  */
object PredictData {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("ModelTrain").setMaster("local")
    sparkConf.set("spark.local.dir", "S:\\Data")
    val sparkContext = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sparkContext)

    val pageViewData = sqlContext.read.parquet("S:\\Kaggle Data\\page_views_join_events_joinTestAD").drop("timestamp").drop("display_id1")


    val learnerParameters = new FM_FTRL_Parameter_Helper
    val learner = new FM_FTRL_Machine("Ad_Click", learnerParameters.getParameterList)
    learner.initUseFilePath()
    learner.loadFMAndWParameters()

    val testData = pageViewData.rdd.collect()
    val outputMap = new util.HashMap[String, util.HashMap[String, Int]]()


    for (row <- testData) {
      val rowString = changeRowToString(row)

      val predictValue = learner.predict(HashUtils.hashDatas(rowString, learnerParameters.getHashSize, "kaggle"))
      if (predictValue > 0.4) {
        val display_id = row(6).toString
        val ad_id = row(7).toString
        if (outputMap.containsKey(display_id)) {
          val map = outputMap.get(display_id)
          if (map.containsKey(ad_id)) {
            map.put(ad_id, map.get(ad_id) + 1)
          }else{
            map.put(ad_id,1)
          }
        }
        else{
          val map = new util.HashMap[String,Int]()
          map.put(ad_id,1);
          outputMap.put(display_id,map)
        }
      }
    }

    println(outputMap.size())


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
