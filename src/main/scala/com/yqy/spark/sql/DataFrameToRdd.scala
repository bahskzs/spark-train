package com.yqy.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * @author bahsk
 * @createTime 2022-02-27 23:41
 * @description 读取dataframe转换rdd进行计算
 * @program: spark-train
 */
object DataFrameToRdd extends App {

  val conf = new SparkConf()
  conf.setAppName("RddToMultiPath")
    .setMaster("local[4]")

  val spark   = SparkSession.builder().config(conf).getOrCreate()


  val sc = spark.sparkContext

   val rdd= spark.read.csv("data/data.txt").rdd

 rdd.map(row => (row(0),row(1),row(2))).collect().foreach(println(_))



}
