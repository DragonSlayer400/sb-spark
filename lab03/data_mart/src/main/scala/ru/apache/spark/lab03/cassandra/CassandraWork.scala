package ru.apache.spark.lab03.cassandra
import org.apache.spark.sql.cassandra._
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

class CassandraWork(val spark: SparkSession) {

    def read(): DataFrame = {
        val tableOpt = Map("table" -> "clients","keyspace" -> "labdata")
        spark
          .read
          .format("org.apache.spark.sql.cassandra")
          .options(tableOpt)
          .load().na.drop("any")
    }

}
