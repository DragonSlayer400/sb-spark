package ru.apache.spark.lab03.hdfs

import java.net.URL
import java.net.URLDecoder.decode

import org.apache.spark.sql.functions.{col, explode, udf}
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.Try

class HdfsWork(val spark: SparkSession) {

  val pathWebLogs = "/labs/laba03/weblogs.json"


  def read(): DataFrame = {
    val getDomain = udf { (url: String) =>
      Try(
        new URL(url)
          .getHost
          .replaceFirst("^(www\\.)","")
      ).toOption
    }
    val weblogs_schema=spark.read.json(pathWebLogs).schema.json
    val newSchema=DataType.fromJson(weblogs_schema).asInstanceOf[StructType]
    val weblogs_df=spark.read.schema(newSchema).json(pathWebLogs)

    weblogs_df.select(col("uid"),explode(col("visits"))).select(col("uid"),col("col.*")).select(col("uid"),getDomain(col("url")).as("domain"))
  }

}
