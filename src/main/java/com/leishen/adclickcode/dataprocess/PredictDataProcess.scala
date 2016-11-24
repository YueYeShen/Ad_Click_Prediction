package com.leishen.adclickcode.dataprocess

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
  * Created by leishen on 2016/11/24 0024.
  */
object PredictDataProcess {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("JoinClickAdData").setMaster("local[4]")

    val sparkContext = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sparkContext)

    val pageJoinEvents = sqlContext.read.load("S:\\Kaggle Data\\page_views_join_events")


    val clickTest = sqlContext.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", true.toString)
      .load("S:\\Kaggle Data\\clicks_test.csv")



    val joinADData = pageJoinEvents.join(clickTest,pageJoinEvents("display_id")===clickTest("display_id"))
    val renameColumn = joinADData.toDF("uuid","document_id","timestamp","platform","geo_location","traffic_source","display_id","display_id1", "ad_id")
    renameColumn.write.save("S:\\Kaggle Data\\page_views_join_events_joinTestAD")
    sparkContext.stop()

  }
}
