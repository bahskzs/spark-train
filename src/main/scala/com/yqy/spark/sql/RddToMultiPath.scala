package com.yqy.spark.sql


import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * @author bahsk
 * @createTime 2022-02-26 17:56
 * @description
 * @program: spark-train
 */
object RddToMultiPath {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setAppName("RddToMultiPath")
      .setMaster("local[4]")
//      .set("","")

    val spark   = SparkSession.builder().config(conf).getOrCreate()


    val sc = spark.sparkContext

//    val rdd = sc.parallelize(List((20220224, 1, 3, 45), (20220224, 3, 4, 55), (20220101, 34, 5, 3)))
    val rdd = sc.parallelize(Array((20220224, 1, 3, 45),  (20220224,3, 4, 55), (20220101,34, 5, 3)))

    // 202202 --> (20220224, 1, 3, 45),(20220224,3, 4, 55)
    rdd.map(
      x => ("pt_d="+x._1.toString.substring(0,6),x._1.toString.concat("," + x._2.toString()).concat("," + x._3).concat(","+x._4))
    ).saveAsHadoopFile("hdfs://master610:9000/data/demo", classOf[String], classOf[String],
      classOf[RDDMultipleTextOutputFormat])




    spark.stop()


  }

}

class RDDMultipleTextOutputFormat extends MultipleTextOutputFormat[Any, Any] {
  override def generateActualKey(key: Any, value: Any): Any =
    NullWritable.get()

  override def generateFileNameForKeyValue(key: Any, value: Any, name: String): String =
    key.toString+"/"+name
}
