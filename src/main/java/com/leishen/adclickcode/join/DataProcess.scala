package com.leishen.adclickcode.join

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by shenlei on 2016/10/26.
  */
object DataProcess {

  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setMaster("local").setAppName("DataProcess")
    val sparkContext = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sparkContext)
    val pageViewSampleData = sqlContext.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", true.toString).
      load("E:\\Kaggle Data\\page_views_sample.csv")


    val eventsData = sqlContext.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", true.toString)
      .load("E:\\Kaggle Data\\events.csv")

    val joinData = pageViewSampleData.join(eventsData, pageViewSampleData("uuid") === eventsData("uuid")
      && pageViewSampleData("document_id") === eventsData("document_id")
      && pageViewSampleData("timestamp") === eventsData("timestamp")
      && pageViewSampleData("platform") === eventsData("platform"))
    joinData.show()

    val renameData = joinData.toDF("uuid", "document_id", "timestamp", "platform", "geo_location", "traffic_source", "display_id"
      , "uuid1", "document_id1", "timestamp1", "platform1", "geo_location1")
    renameData.show()
    renameData.write.save("page_views_sample_join_events")


    val filterData = renameData.select("uuid", "document_id", "timestamp", "platform", "geo_location", "traffic_source", "display_id")

    filterData.show()
    filterData.write.save("E:\\Kaggle Data\\page_views_sample_join_events")

    val data = sqlContext.read.load("E:\\Kaggle Data\\page_views_sample_join_events")

    data.show()

    sparkContext.stop()
  }
}
