package com.leishen.adclickcode.trainkandpredict

import java.util

import com.leishen.adclickcode.ftrl.{FM_FTRL_Machine, FM_FTRL_Parameter_Helper}
import com.leishen.adclickcode.utils.HashUtils
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.metrics.source.Source
import org.apache.spark.{SparkConf, SparkContext, TaskContext}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.util.{TaskCompletionListener, TaskFailureListener}

/**
  * Created by leishen on 2016/11/24
  */
object PredictData {
  val builder = new StringBuilder
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("ModelTrain").setMaster("local")
    sparkConf.set("spark.local.dir", "S:\\Data")

    val sparkContext = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sparkContext)

    val pageViewData = sqlContext.read.parquet("S:\\Kaggle Data\\page_views_join_events_joinTestAD").drop("timestamp").drop("display_id1")

    val allRows= pageViewData.rdd.map(changeRowToString(_)).take(10000000)
    println(allRows.length)








    /*val learnerParameters = new FM_FTRL_Parameter_Helper
    val learner = new FM_FTRL_Machine("Ad_Click", learnerParameters.getParameterList)
    learner.initUseFilePath()
    learner.loadFMAndWParameters()



    val outputMap = new util.HashMap[String, util.HashMap[String, Int]]()

    for(row <- allRows){
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

    println(outputMap.size())*/


  }

  private def changeRowToString(row: Row): String = {
    builder.setLength(0)
    for (i <- 0 to row.length - 2) {
      builder.append(row(i) + ";")
    }
    builder.append(row(row.length - 1))

    builder.toString()

  }

}
