package com.yqy.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
 * @author bahsk
 * @createTime 2022-02-26 19:20
 * @description
 * @program: spark-train
 */
object RddToDataFrame extends App {
  val conf = new SparkConf()
  conf.setAppName("RddToMultiPath")
    .setMaster("local[4]")

  val spark   = SparkSession.builder().config(conf).getOrCreate()


  val sc = spark.sparkContext

  val movieSchema = StructType(Array(StructField("actor_name", StringType, true),
    StructField("movie_title", StringType, true),
    StructField("produced_year", StringType, true),
    StructField("movie_order", StringType, true)
  ))

  // 正则模式读取文件夹
  val df = spark.read.option("header","false").schema(movieSchema).csv("hdfs://master610:9000/data/demo/pt_d=2022[0-1][1-9]/*")
  df.show()
  df.write.mode(SaveMode.Overwrite).orc("hdfs://master610:9000/user/hive/warehouse/demo/pt_d=202202")
//  df.repartition(2).write.orc("data/demo")
  spark.stop()
}
