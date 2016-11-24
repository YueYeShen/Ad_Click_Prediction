package com.leishen.adclickcode.dataprocess

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by shenlei on 2016/10/26.
  */
object DataProcess {

  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setMaster("local[4]").setAppName("DataProcess")
    sparkConf.set("spark.local.dir","S:\\Data")
    val sparkContext = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sparkContext)

    val pageViewData = sqlContext.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", true.toString).
      load("S:\\Kaggle Data\\page_views.csv")


    val eventsData = sqlContext.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", true.toString)
      .load("S:\\Kaggle Data\\events.csv")

    val joinData = pageViewData.join(eventsData, pageViewData("uuid") === eventsData("uuid")
      && pageViewData("document_id") === eventsData("document_id")
      && pageViewData("timestamp") === eventsData("timestamp")
      && pageViewData("platform") === eventsData("platform"))


    val renameData = joinData.toDF("uuid", "document_id", "timestamp", "platform", "geo_location", "traffic_source", "display_id"
      , "uuid1", "document_id1", "timestamp1", "platform1", "geo_location1")


    val filterData = renameData.select("uuid", "document_id", "timestamp", "platform", "geo_location", "traffic_source", "display_id")


    filterData.write.save("S:\\Kaggle Data\\page_views_join_events")

    sparkContext.stop()
  }
}
