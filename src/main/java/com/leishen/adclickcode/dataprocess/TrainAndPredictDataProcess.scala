package com.leishen.adclickcode.dataprocess

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by leishen on 2016/12/7 0007.
  */
object TrainAndPredictDataProcess {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("TrainPredictProcess")
    val sparkContext = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sparkContext)

    val adClick = sqlContext.read.load("S:\\Kaggle Data\\page_views_join_events_joinAD")
      .drop("timestamp").drop("display_id1")

    println(adClick.count())

    adClick.rdd.coalesce(15).saveAsTextFile("S:\\Kaggle Data\\trainData")

    val pageViewData = sqlContext.read.parquet("S:\\Kaggle Data\\page_views_join_events_joinTestAD")
      .drop("timestamp").drop("display_id1")

    pageViewData.rdd.coalesce(25).saveAsTextFile("S:\\Kaggle Data\\testData")

    sparkContext.stop()
  }

}
