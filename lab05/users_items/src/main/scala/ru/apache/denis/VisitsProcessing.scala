package ru.apache.denis

import org.apache.spark.sql
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.functions._
import org.apache.spark.sql.sources.In


class VisitsProcessing(spark: SparkSession, inputDir: String, outputDir: String){

  def update_off() ={
    saveResult(readNew(),findMax())
  }

  def update_on() = {
    val dfNew = readNew()
    val dfOld = spark.read.parquet(s"$outputDir/*")
    val unDF = dfNew.union(dfOld).groupBy(col("uid")).sum()
    val renamedColumn = unDF.columns.map(name => col(name).as(name.replaceAll("^sum\\(","").replaceAll("\\)$","")))
    val unDFAll = unDF.select(renamedColumn: _* )
    saveResult(unDFAll, findMax())
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

  def findMax(): Int = {
    import spark.implicits._
    val dfBuy = spark.read.format("json").option("path", s"$inputDir/buy").load.na.drop("any")
    val dfView = spark.read.format("json").option("path", s"$inputDir/view").load.na.drop("any")
    val max_v = dfView.selectExpr("max(date_rep)").as[Int].collect()(0)
    val max_b = dfBuy.selectExpr("max(date_rep)").as[Int].collect()(0)
    if(max_b > max_v) max_b else max_v
  }

  def saveResult(df: DataFrame, maxDate: Int) = {
    df.write.mode("overwrite").parquet(s"$outputDir/$maxDate")
  }

}
