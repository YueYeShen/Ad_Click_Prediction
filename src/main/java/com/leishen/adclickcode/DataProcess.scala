package com.leishen.adclickcode

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
    val kaggleData = sqlContext.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", true.toString)
      .load("E:\\BaiduYunDownload\\Kaggle Data\\page_views_sample.csv")


    kaggleData.show()
    sparkContext.stop()

  }

}
