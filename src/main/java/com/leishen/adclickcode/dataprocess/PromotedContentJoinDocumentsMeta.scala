package com.leishen.adclickcode.dataprocess

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
  * Created by leishen on 2016/12/17 0017.
  */
object PromotedContentJoinDocumentsMeta {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[4]").setAppName("DataProcess")

    val sparkContext = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sparkContext)

    val promotedContent = sqlContext.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", true.toString)
      .load("S:\\Kaggle Data\\promoted_content.csv")

    val documentsMeta = sqlContext.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", true.toString)
      .load("S:\\Kaggle Data\\documents_meta.csv")

    val documentsTopics = sqlContext.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", true.toString)
      .load("S:\\Kaggle Data\\documents_topics.csv")

    val documentsCategories = sqlContext.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", true.toString)
      .load("S:\\Kaggle Data\\documents_categories.csv")

    val result = promotedContent.join(documentsMeta, promotedContent("document_id") === documentsMeta("document_id"))

    println(result.count())

    val renameDF = result.toDF("ad_id", "document_id", "campaign_id", "advertiser_id", "document_id1", "source_id", "publisher_id", "publish_time")


    val afterRenameDF = renameDF.drop("document_id1")
    val DMP = documentsTopics.join(documentsCategories, documentsTopics("document_id") === documentsCategories("document_id"))

    val afterDMP = DMP.toDF("document_id", "topic_id", "confidence_level", "document_id1", "category_id", "confidence_level1").drop("document_id1").drop( "confidence_level1")
    val reallyResult = afterRenameDF.join(afterDMP, afterRenameDF("document_id") === afterDMP("document_id"))
    val finalResult = reallyResult.toDF("ad_id","document_id","campaign_id","advertiser_id","source_id","publisher_id","publish_time","document_id1","topic_id","confidence_level","category_id")
      .drop("document_id1").drop("publish_time")
    finalResult.show()
    finalResult.write.save("S:\\Kaggle Data\\Detail")

    sparkContext.stop()

  }

}
