package com.selflearning.veluhouse.sparkexamples

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext



object WordCount {
  def main(args: Array[String]): Unit = {

  println ("Hello World")

  val conf = new SparkConf()
              .setAppName("WordCount")
              .setMaster("local")
              
  val sc = new SparkContext(conf)
  
  val txtfile = sc.textFile("C:/Users/ADMIN/Downloads/samplefile.txt")
  
  val count= txtfile.flatMap { x => x.split(" ") }
                  .map { word => (word,1) }
                  .reduceByKey(_+_)
  
//   txtfile.foreach { x => ??? }
                  println(count)
   
  }
}