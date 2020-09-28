package ru.apache.spark.lab03.postgres

import org.apache.spark.sql.{DataFrame, SparkSession}

class PostgresWork(val spark: SparkSession) {
  val jdbcUrl = "jdbc:postgresql://10.0.0.5:5432/labdata?user=denis_nurdinov&password=cwSXvVde"



  def read(): DataFrame = {
    spark.read.format("jdbc").option("url", jdbcUrl).option("dbtable", "domain_cats").option("driver", "org.postgresql.Driver").load().na.drop("any")
  }

  def write(dataframe: DataFrame, jdbcUrl: String): Unit = {
    dataframe.write.format("jdbc").option("url", jdbcUrl).option("dbtable", "clients").option("driver", "org.postgresql.Driver").mode("overwrite").save
  }


}
