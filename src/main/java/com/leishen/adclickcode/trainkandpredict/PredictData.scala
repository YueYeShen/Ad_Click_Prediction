package com.leishen.adclickcode.trainkandpredict

import java.io.{BufferedWriter, FileWriter}
import java.util

import com.leishen.adclickcode.ftrl.{FM_FTRL_Machine, FM_FTRL_Parameter_Helper}
import com.leishen.adclickcode.utils.HashUtils
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.metrics.source.Source
import org.apache.spark.{SparkConf, SparkContext, TaskContext}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.util.{TaskCompletionListener, TaskFailureListener}

import scala.io.Source

/**
  * Created by leishen on 2016/11/24
  */
object PredictData {
  val builder = new StringBuilder

  def main(args: Array[String]): Unit = {


    val learnerParameters = new FM_FTRL_Parameter_Helper


    val learner = new FM_FTRL_Machine("Ad_Click", learnerParameters.getParameterList)
    learner.initUseFilePath()
    learner.loadFMAndWParameters()


    val testDataDir = "S:\\Kaggle Data\\testData\\"
    val resultFilePath = "S:\\Ad_Click_Prediction\\Resource\\result.txt"

    var allRowCount = 0
    var correctRowCount = 0
    var _1Count = 0
    val filePre = "part-000"
    var filePath = new String("")
    var ii = 0

    val writer = new BufferedWriter(new FileWriter(resultFilePath))
    for (i <- 0 until 25) {


      if (i < 10) {
        filePath = testDataDir + filePre + "0" + i
      }
      else {
        filePath = testDataDir + filePre + i
      }

      Source.fromFile(filePath).getLines().foreach(line => {
        if (ii < 24000000) {
          var replaceLine = line.replaceAll("\\[", "").replaceAll("\\]", "")
          var lineSplits = replaceLine.trim.split(",")
          var hashValues = HashUtils.hashDatas(lineSplits, learnerParameters.getHashSize, "kaggle")
          var predictValue = learner.predict(hashValues);

          if (predictValue > 0.4) {

            val displayId = lineSplits(lineSplits.length - 2)

            val adId = lineSplits(lineSplits.length - 1)

            writer.write(displayId + "," + adId + "\r\n")
          }

          ii = ii + 1

        }
      }
      )

    }
    writer.close()
    /*



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


}
