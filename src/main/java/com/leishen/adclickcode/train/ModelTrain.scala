package com.leishen.adclickcode.train

import com.leishen.adclickcode.ftrl.{HashUtils, FM_FTRL_Parameter_Helper, FM_FTRL_machine}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by shenlei on 2016/10/29.
  */
object ModelTrain {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("ModelTrain").setMaster("local[4]")
    val sparkContext = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sparkContext)

    val adClick = sqlContext.read.load("E:\\Kaggle Data\\page_views_sample_join_events_joinAD")
      .drop("timestamp").drop("display_id1")
    adClick.show()
    val allRows = adClick.rdd.collect()

    val learnerParameters = new FM_FTRL_Parameter_Helper
    val learner = new FM_FTRL_machine("Ad_Click", learnerParameters.getParameterList);
    var allRowCount = 0
    var correctRowCount = 0
    var _1Count = 0

    for (i <- 0 to 5) {

      if (i == 5)
        for (row <- allRows) {
          allRowCount = allRowCount + 1
          val label = row(row.length - 1).toString.toDouble
          val rowValue = changeRowToString(row)
          val hashValues = HashUtils.hashDatas(rowValue, learnerParameters.getHashSize, "kaggle")
          val p = learner.predict(hashValues);

          /* println("label is : "+label)
           println("predict value is : "+p)*/

          if (label == 1)
            _1Count = _1Count + 1
          if (p > 0.5 && label == 1)
            correctRowCount = correctRowCount + 1

          val loss = learner.logLoss(p, label);
          learner.update(hashValues, p, label);
        }
      else {
        for (row <- allRows) {

          val label = row(row.length - 1).toString.toDouble
          val rowValue = changeRowToString(row)
          val hashValues = HashUtils.hashDatas(rowValue, learnerParameters.getHashSize, "kaggle")
          val p = learner.predict(hashValues);

          val loss = learner.logLoss(p, label);
          learner.update(hashValues, p, label);
        }
      }

    }

    println("allCount Number : " + allRowCount)
    println("correctRowCount : " + correctRowCount)
    println("label 1 : " + _1Count)
  }

  private def changeRowToString(row: Row): String = {
    val builder = new StringBuilder
    for (i <- 0 to row.length - 3) {
      builder.append(row(i) + ";")
    }
    builder.append(row(row.length - 2))
    builder.toString()
  }
}
