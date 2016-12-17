package com.leishen.adclickcode.trainkandpredict

import java.io.{BufferedWriter, FileWriter}

import com.leishen.adclickcode.ftrl.{FM_FTRL_Machine, FM_FTRL_Parameter_Helper}
import com.leishen.adclickcode.utils.HashUtils

import scala.io.Source

/**
  * Created by leishen on 2016/12/16 0016.
  */
object DataTest {
  def main(args: Array[String]): Unit = {
    val learnerParameters = new FM_FTRL_Parameter_Helper
    val learner = new FM_FTRL_Machine("Ad_Click", learnerParameters.getParameterList)
    learner.initUseFilePath()
    learner.loadFMAndWParameters()


    val testDataDir = "S:\\Kaggle Data\\trainData\\"


    val filePre = "part-000"
    var filePath = new String("")
    var ii = 0
    var correct = 0

    for (i <- 6 until 9) {

      System.out.println("hello");
      if (i < 10) {
        filePath = testDataDir + filePre + "0" + i
      }
      else {
        filePath = testDataDir + filePre + i
      }

      Source.fromFile(filePath).getLines().foreach(line => {
        val replaceLine = line.replaceAll("\\[", "").replaceAll("\\]", "")
        val lineSplits = replaceLine.trim.split(",")
        val label = lineSplits(lineSplits.length - 1).toDouble
        val datas = for (i <- 0 until lineSplits.length - 1) yield lineSplits(i)
        val hashValues = HashUtils.hashDatas(datas.toArray, learnerParameters.getHashSize, "kaggle")
        val predictValue = learner.predict(hashValues);
        ii = ii + 1
        println(ii)
        if (predictValue > 0.4 && label == 1) {
          correct = correct + 1
        }
      }
      )

    }
    println("all count : " + ii)
    println("correct : " + correct)

  }

}
