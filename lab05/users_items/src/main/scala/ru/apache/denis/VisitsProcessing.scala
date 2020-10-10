package ru.apache.denis

import java.text.SimpleDateFormat

import org.apache.spark.sql
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.functions._
import org.apache.spark.sql.sources.In


class VisitsProcessing(spark: SparkSession, inputDir: String, outputDir: String){

  def update_off() ={
    saveResult(readNew(),maxDate())
  }

  def update_on() = {
    val buy = udf { (category: String) =>
      "buy_" + category.replaceAll(" ", "_").replaceAll("-", "_").toLowerCase
    }
    val view = udf { (category: String) =>
      "view_" + category.replaceAll(" ", "_").replaceAll("-", "_").toLowerCase
    }
    val dfBuy = spark.read.format("json").option("path", s"$inputDir/buy").load.na.drop("any").select(col("uid"),buy(col("item_id")).as("category"))
    val dfView = spark.read.format("json").option("path", s"$inputDir/view").load.na.drop("any").select(col("uid"),view(col("item_id")).as("category"))
    val result = dfBuy.union(dfView).union(readOld()).groupBy(col("uid")).pivot("category").count().na.fill(0)
    saveResult(result, maxDate())
  }

  def readOld()= {
    val dfOld = spark.read.parquet(s"$outputDir/*")
    val nm = dfOld.columns.toList.filter(_ != "uid")
    val nm_dbl = nm.map(s => "\"" + s + "\", " + s).mkString(", ")
    val s = "stack(" + nm.length.toString + ", " + nm_dbl + ") as (category, value)"
    val old_users_items = dfOld.selectExpr("uid", s)
    old_users_items.select(col("uid"),col("category"))
  }

  def maxDate() = {
    val dfBuy = spark.read.format("json").option("path", s"$inputDir/buy").load.na.drop("any")
    val dfView = spark.read.format("json").option("path", s"$inputDir/view").load.na.drop("any")
    import spark.implicits._
    val format = new SimpleDateFormat("yyyyMMdd")
    format.setTimeZone(java.util.TimeZone.getTimeZone("UTC"))
    val max_v = dfView.selectExpr("max(timestamp)").as[Long].collect()(0)
    val max_b = dfBuy.selectExpr("max(timestamp)").as[Long].collect()(0)
    format.format(if (max_v > max_b) max_v else max_b)
  }

  def readNew() = {
    val buy = udf { (category: String) =>
      "buy_" + category.replaceAll(" ", "_").replaceAll("-", "_").toLowerCase
    }
    val view = udf { (category: String) =>
      "view_" + category.replaceAll(" ", "_").replaceAll("-", "_").toLowerCase
    }
    val dfBuy = spark.read.format("json").option("path", s"$inputDir/buy").load.na.drop("any")
    val dfView = spark.read.format("json").option("path", s"$inputDir/view").load.na.drop("any")
    val formattedDfView = dfView.select(col("uid"),view(col("item_id")).as("category")).groupBy(col("uid")).pivot("category").count()
    val formattedDfBuy = dfBuy.select(col("uid"),buy(col("item_id")).as("category")).groupBy(col("uid")).pivot("category").count()
    formattedDfView.join(formattedDfBuy, Seq("uid"),"left").na.fill(0)
  }



  def saveResult(df: DataFrame, maxDate: String) = {
    df.write.mode("overwrite").parquet(s"$outputDir/$maxDate")
  }

}
