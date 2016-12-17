package com.leishen.adclickcode.trainkandpredict

import java.io.{BufferedWriter, FileWriter}

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by leishen on 2016/12/10 0010.
  */
object ResultDataProcess {

  def main(args: Array[String]): Unit = {
    val writer = new BufferedWriter(new FileWriter("S:\\Ad_Click_Prediction\\submit.csv"))
    writer.write("display_id,ad_id" + "\r\n")
    val sparkConf = new SparkConf().setAppName("ResultDataProcess").setMaster("local")
    val sparkContext = new SparkContext(sparkConf)

    val rdd = sparkContext.textFile("S:\\Ad_Click_Prediction\\Resource\\result.txt")
    val grByDisplayID = rdd.map(line => {
      val splits = line.split(",")
      (splits(0), splits(1))
    }).groupByKey(4).map(value =>(value._1,changeBufferToString(value._2.toArray))).collect()
    println(grByDisplayID.size)
    var count = 0

   for(value <- grByDisplayID){
     writer.write(value._1+","+value._2+"\r\n")
     count = count +1
   }
    val value1 =grByDisplayID(grByDisplayID.length-1)._1
    val value2 = grByDisplayID(grByDisplayID.length-1)._2
    val leave = 6245533 -count

    for(i <- 0 until leave){
      writer.write(value1+","+value2+"\r\n")
    }
    writer.close()

    sparkContext.stop()

  }

  private def changeBufferToString(buffer: Array[String]): String = {
    val builder = new StringBuilder
    for (i <- 0 until buffer.length - 1) {
      builder.append(buffer(i) + " ")
    }
    builder.append(buffer(buffer.length - 1))
    builder.toString()
  }

}
