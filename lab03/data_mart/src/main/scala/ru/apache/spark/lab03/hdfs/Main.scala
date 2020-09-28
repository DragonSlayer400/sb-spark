package ru.apache.spark.lab03.hdfs

import java.net.URL
import java.net.URLDecoder.decode
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SparkSession, types}
import org.apache.spark.sql.types.{DataType, LongType, StringType, StructField, StructType}

import scala.util.Try

object Main {
  def main(args: Array[String]): Unit = {

    val getDomain = udf { (url: String) =>
      Try(
        new URL(decode(url))
          .getHost
          .replaceFirst("^(www\\.)","")
      ).toOption
    }


    val sparkConf = new SparkConf().setAppName("lab03").setMaster("local[*]")

    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val weblogs_schema=spark.read.json("C:\\Users\\denis\\Desktop\\weblogs.json").schema.json
    val newSchema=DataType.fromJson(weblogs_schema).asInstanceOf[StructType]
    val weblogs_df=spark.read.schema(newSchema).json("C:\\Users\\denis\\Desktop\\weblogs.json")
    weblogs_df.select(col("uid"),explode(col("visits"))).select(col("uid"),col("col.*")).groupBy("uid","url").count().select(col("uid"),getDomain(col("url")).as("domain"),col("count")).show(10)
    weblogs_df.printSchema()



  }
}
