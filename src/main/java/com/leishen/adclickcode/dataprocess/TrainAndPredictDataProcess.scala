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
      .drop("timestamp").drop("display_id1").drop("uuid")

    val detail = sqlContext.read.load("S:\\Kaggle Data\\Detail")
    val detailTrainJoinAdClick = detail.join(adClick, detail("document_id") === adClick("document_id"))
    // ad_id|document_id|campaign_id|advertiser_id|source_id|publisher_id|topic_id|category_id|platform|geo_location|traffic_source|display_id|clicked|
    val trainResult = detailTrainJoinAdClick.toDF("ad_id", "document_id", "campaign_id", "advertiser_id", "source_id", "publisher_id", "topic_id", "confidence_level", "category_id", "document_id1", "platform", "geo_location", "traffic_source", "display_id", "ad_id1", "clicked")
      .drop("document_id1").drop("confidence_level").drop("ad_id1")


    trainResult.rdd.coalesce(15).saveAsTextFile("S:\\Kaggle Data\\trainData")


    val pageViewData = sqlContext.read.parquet("S:\\Kaggle Data\\page_views_join_events_joinTestAD")
      .drop("timestamp").drop("display_id1").drop("uuid")

    //| ad_id|document_id|campaign_id|advertiser_id|source_id|publisher_id|topic_id|category_id|platform|geo_location|traffic_source|display_id|
    val detailTestData = detail.join(pageViewData, detail("document_id") === pageViewData("document_id"))
    val testResult = detailTestData.toDF("ad_id", "document_id", "campaign_id", "advertiser_id", "source_id", "publisher_id", "topic_id", "confidence_level", "category_id", "document_id1", "platform", "geo_location", "traffic_source", "display_id", "ad_id1")
      .drop("document_id1").drop("confidence_level").drop("ad_id1")

    testResult.rdd.coalesce(15).saveAsTextFile("S:\\Kaggle Data\\testData")
    sparkContext.stop()
  }

}
