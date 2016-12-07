package com.leishen.adclickcode.trainkandpredict

import com.leishen.adclickcode.ftrl.{FM_FTRL_Machine, FM_FTRL_Parameter_Helper}
import com.leishen.adclickcode.utils.HashUtils
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.io.Source

/**
  * Created by shenlei on 2016/10/29.
  */
object ModelTrain {
  def main(args: Array[String]) {



    val trainDataDir = "S:\\Kaggle Data\\trainData\\"
    val learnerParameters = new FM_FTRL_Parameter_Helper
    val learner = new FM_FTRL_Machine("Ad_Click", learnerParameters.getParameterList);
    var allRowCount = 0
    var correctRowCount = 0
    var _1Count = 0
    val filePre = "part-000"
    var filePath = ""
    for (i <- 0 until 15) {
      for (j <- 0 to 5) {

        if (i < 10) {
          filePath = trainDataDir + filePre + "0" + i
        }
        else {
          filePath = trainDataDir + filePre + i
        }

        Source.fromFile(filePath).getLines().foreach(line => {
          val replaceLine = line.replaceAll("\\[", "").replaceAll("\\]", "")
          val lineSplits = replaceLine.trim.split(",")
          val label = lineSplits(lineSplits.length - 1).toDouble
          val datas = for (i <- 0 until lineSplits.length - 1) yield lineSplits(i)
          val hashValues = HashUtils.hashDatas(datas.toArray, learnerParameters.getHashSize, "kaggle")
          val p = learner.predict(hashValues);

          val loss = learner.logLoss(p, label);
          learner.update(hashValues, p, label);
        }
        )
      }
    }
    learner.initUseFilePath

    learner.write_n()
    learner.write_w()
    learner.write_z()
    learner.write_n_fm()
    learner.write_w_fm()
    learner.write_z_fm()

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
