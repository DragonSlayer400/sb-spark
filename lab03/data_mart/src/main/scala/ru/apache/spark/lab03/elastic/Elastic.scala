package ru.apache.spark.lab03.elastic

import org.apache.spark.sql.{DataFrame, SparkSession}

class Elastic(val spark: SparkSession) {

  def read(): DataFrame = {
    val esOpt =
      Map(
        "es.nodes" -> "10.0.0.5:9200",
        "es.batch.write.refresh" -> "false",
        "es.nodes.wan.only" -> "true"
      )
    spark.read.format("es").options(esOpt).load("visits").na.drop("any")
  }

}
