package com.leishen.adclickcode

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by shenlei on 2016/10/28.
  */
object JoinClickAdData {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("JoinClickAdData").setMaster("local[4]")
    val sparkContext = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sparkContext)

    val pageDisplayData = sqlContext.read.load("E:\\Kaggle Data\\page_views_sample_join_events")

    pageDisplayData.show()

    val clickADData = sqlContext.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", true.toString)
      .load("E:\\Kaggle Data\\clicks_train.csv")

    clickADData.show()

    val joinADData = pageDisplayData.join(clickADData,pageDisplayData("display_id")===clickADData("display_id"))
    val renameColumn = joinADData.toDF("uuid","document_id","timestamp","platform","geo_location","traffic_source","display_id","display_id1", "ad_id","clicked")
    renameColumn.write.save("page_views_sample_join_events_joinAD")
    sparkContext.stop()
    
  }

}
