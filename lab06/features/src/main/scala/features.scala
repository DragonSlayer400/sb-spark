import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.{DataType, DataTypes, StructType, TimestampType}
import org.apache.spark.sql.functions._


object features {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().getOrCreate()
    val pathWebLogs = "/labs/laba03//weblogs.json"
    val userItemsPath = "/user/denis.nurdinov/users-items/20200429";
    val weblogs_schema = spark.read.json(pathWebLogs).schema.json
    val newSchema = DataType.fromJson(weblogs_schema).asInstanceOf[StructType]
    val weblogs_df = spark.read.schema(newSchema).json(pathWebLogs).na.drop("any")
    val userItems = spark.read.parquet(userItemsPath)
    val usersLogs = weblogs_df.select(col("uid"),explode(col("visits")))
      .select(col("uid"),col("col.*"))
      .withColumn("host", lower(callUDF("parse_url", col("url"), lit("HOST"))))
      .withColumn("domain", regexp_replace(col("host"), "www.", ""))
      .select(col("uid"),col("timestamp"),col("domain"))
      .na.drop("any")

    val domain_1000 = usersLogs
      .groupBy(col("domain"))
      .count
      .orderBy(col("count").desc)
      .na.drop("any")
      .limit(1000)
      .orderBy(col("domain").asc)
      .select(col("domain"))

    val userCountDay = usersLogs.select(col("uid"),date_format(to_utc_timestamp((col("timestamp") / 1000).cast(TimestampType), "Europe/Moscow"),"EEE").as("day")).groupBy(col("uid")).pivot(col("day")).count().na.fill(0)


    val userCountHour = usersLogs.select(col("uid"), hour(to_utc_timestamp((col("timestamp") / 1000).cast(TimestampType), "Europe/Moscow")).as("hour")).groupBy(col("uid")).pivot(col("hour")).count().na.fill(0)
      .withColumn("web_work_hours", col("9")+col("10")+col("11")+col("12")+col("13")+col("14")+col("15")+col("16")+col("17"))
      .withColumn("web_evening_hours", col("18")+col("19")+col("20")+col("21")+col("22")+col("23"))
      .withColumn("web_all_hours", col("web_work_hours") + col("web_evening_hours") + col("0") + col("1") + col("2") + col("3") + col("4") + col("5") + col("6") + col("7") + col("8"))
      .withColumn("web_fraction_work_hours", col("web_work_hours").cast(DataTypes.DoubleType) / col("web_all_hours").cast(DataTypes.DoubleType))
      .withColumn("web_fraction_evening_hours", col("web_evening_hours").cast(DataTypes.DoubleType) / col("web_all_hours").cast(DataTypes.DoubleType))
      .drop(col("web_work_hours")).drop(col("web_evening_hours")).drop(col("web_all_hours"))


    val renamedColumnHours = userCountHour.columns.map(name => col(name).as(s"web_hour_$name"))

    val userHours = userCountHour.select(renamedColumnHours: _*)
      .withColumnRenamed("web_hour_uid", "uid")
      .withColumnRenamed("web_hour_web_fraction_work_hours","web_fraction_work_hours")
      .withColumnRenamed("web_hour_web_fraction_evening_hours","web_fraction_evening_hours")

    val renamedColumnDay = userCountDay.columns.map(name => col(name).as(s"web_day_$name".toLowerCase))

    val userDays = userCountDay.select(renamedColumnDay:_*).withColumnRenamed("web_day_uid","uid")

    val domain_uid = usersLogs
      .filter(col("domain").isin(domain_1000.collect().map(_(0)).toList:_*))
      .orderBy(col("domain").asc).groupBy(col("uid"), col("domain")).count()
      .groupBy(col("uid")).pivot("domain").agg(sum("count")).na.fill(0)

    val domainUidFeature = domain_uid.select(col("uid"), array(domain_uid.columns.drop(1).map(c => col(s"`$c`")):_*).alias("domain_features"))


    val result = userItems.join(domainUidFeature, Seq("uid"), "left").join(userDays, Seq("uid"), "left").join(userHours, Seq("uid"), "left").na.fill(0)


    result.write.mode("overwrite").parquet("/user/denis.nurdinov/features")

  }
}
