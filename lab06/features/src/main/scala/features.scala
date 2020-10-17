import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.functions._


object features {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    val pathWebLogs = "/labs/laba03/weblogs.json"
    val pathUsersItemsMatrix = "/user/denis.nurdinov/users-items/20200429/*";
    val outputPath = "/user/denis.nurdinov/features";

    val weblogs_schema = spark.read.json(pathWebLogs).schema.json
    val newSchema = DataType.fromJson(weblogs_schema).asInstanceOf[StructType]
    newSchema.printTreeString()
    val weblogs_df=spark.read.schema(newSchema).json(pathWebLogs).na.drop("any")
    val usersItems = spark.read.parquet(pathUsersItemsMatrix);

    val domain_uid_raw = weblogs_df
      .select(col("uid"),explode(col("visits")))
      .select(col("uid"),col("col.*"))
      .withColumn("host", lower(callUDF("parse_url", $"url", lit("HOST"))))
      .withColumn("domain", regexp_replace($"host", "www.", ""))
    val domain_1000 = domain_uid_raw
      .groupBy(col("domain"))
      .count
      .orderBy(col("count").desc)
      .na.drop("any")
      .limit(1000)
      .orderBy(col("domain").asc)
      .select(col("domain"))
    val domain_uid = domain_uid_raw
      .select(col("uid"), col("timestamp"), col("domain"))
      .filter(col("domain").isin(domain_1000.collect().map(_(0)).toList:_*))
    val groupDF = domain_uid.orderBy(col("domain").asc).groupBy(col("uid"), col("domain")).count()
    val finalDF = groupDF.groupBy(col("uid")).pivot("domain").agg(sum("count")).na.fill(0)
    val columns = finalDF.columns.drop(1).map(c => col(s"`$c`"));
    val result = finalDF.select(col("uid"), array(columns:_*).as("domain_features"))
    import org.apache.spark.sql.expressions.Window
    val windowUid = Window.partitionBy("uid")
    val uid_data_transform = domain_uid_raw
      .select(col("uid"), col("timestamp"), col("domain"))
      .withColumn("datetime", to_utc_timestamp(from_unixtime(col("timestamp")/1000,"yyyy-MM-dd HH:mm:ss"),"Europe/Moscow"))
      .withColumn("web_day", lower(date_format('datetime, "E")))
      .withColumn("web_hour", hour(col("datetime")))
      .withColumn("web_day_mon", when(lower(date_format('datetime, "E")) === "mon", 1).otherwise(0))
      .withColumn("web_day_tue", when(lower(date_format('datetime, "E")) === "tue", 1).otherwise(0))
      .withColumn("web_day_wed", when(lower(date_format('datetime, "E")) === "wed", 1).otherwise(0))
      .withColumn("web_day_thu", when(lower(date_format('datetime, "E")) === "thu", 1).otherwise(0))
      .withColumn("web_day_fri", when(lower(date_format('datetime, "E")) === "fri", 1).otherwise(0))
      .withColumn("web_day_sat", when(lower(date_format('datetime, "E")) === "sat", 1).otherwise(0))
      .withColumn("web_day_sun", when(lower(date_format('datetime, "E")) === "sun", 1).otherwise(0))
      .withColumn("web_hour_0", when(hour(col("datetime")) === 0, 1).otherwise(0))
      .withColumn("web_hour_1", when(hour(col("datetime")) === 1, 1).otherwise(0))
      .withColumn("web_hour_2", when(hour(col("datetime")) === 2, 1).otherwise(0))
      .withColumn("web_hour_3", when(hour(col("datetime")) === 3, 1).otherwise(0))
      .withColumn("web_hour_4", when(hour(col("datetime")) === 4, 1).otherwise(0))
      .withColumn("web_hour_5", when(hour(col("datetime")) === 5, 1).otherwise(0))
      .withColumn("web_hour_6", when(hour(col("datetime")) === 6, 1).otherwise(0))
      .withColumn("web_hour_7", when(hour(col("datetime")) === 7, 1).otherwise(0))
      .withColumn("web_hour_8", when(hour(col("datetime")) === 8, 1).otherwise(0))
      .withColumn("web_hour_9", when(hour(col("datetime")) === 9, 1).otherwise(0))
      .withColumn("web_hour_10", when(hour(col("datetime")) === 10, 1).otherwise(0))
      .withColumn("web_hour_11", when(hour(col("datetime")) === 11, 1).otherwise(0))
      .withColumn("web_hour_12", when(hour(col("datetime")) === 12, 1).otherwise(0))
      .withColumn("web_hour_13", when(hour(col("datetime")) === 13, 1).otherwise(0))
      .withColumn("web_hour_14", when(hour(col("datetime")) === 14, 1).otherwise(0))
      .withColumn("web_hour_15", when(hour(col("datetime")) === 15, 1).otherwise(0))
      .withColumn("web_hour_16", when(hour(col("datetime")) === 16, 1).otherwise(0))
      .withColumn("web_hour_17", when(hour(col("datetime")) === 17, 1).otherwise(0))
      .withColumn("web_hour_18", when(hour(col("datetime")) === 18, 1).otherwise(0))
      .withColumn("web_hour_19", when(hour(col("datetime")) === 19, 1).otherwise(0))
      .withColumn("web_hour_20", when(hour(col("datetime")) === 20, 1).otherwise(0))
      .withColumn("web_hour_21", when(hour(col("datetime")) === 21, 1).otherwise(0))
      .withColumn("web_hour_22", when(hour(col("datetime")) === 22, 1).otherwise(0))
      .withColumn("web_hour_23", when(hour(col("datetime")) === 23, 1).otherwise(0))
      .withColumn("web_work_hours", when((col("web_hour") >= 9) && (col("web_hour") < 18), 1).otherwise(0))
      .withColumn("web_evening_hours", when((col("web_hour") >= 18) && (col("web_hour") < 24), 1).otherwise(0))
      .withColumn("cnt_uid", count("*").over(windowUid))
    val uid_data_transform_agg = uid_data_transform
      .groupBy(col("uid")).agg(
      sum("web_day_mon").alias("web_day_mon"),
      sum("web_day_tue").alias("web_day_tue"),
      sum("web_day_wed").alias("web_day_wed"),
      sum("web_day_thu").alias("web_day_thu"),
      sum("web_day_fri").alias("web_day_fri"),
      sum("web_day_sat").alias("web_day_sat"),
      sum("web_day_sun").alias("web_day_sun"),
      sum("web_hour_0").alias("web_hour_0"),
      sum("web_hour_1").alias("web_hour_1"),
      sum("web_hour_2").alias("web_hour_2"),
      sum("web_hour_3").alias("web_hour_3"),
      sum("web_hour_4").alias("web_hour_4"),
      sum("web_hour_5").alias("web_hour_5"),
      sum("web_hour_6").alias("web_hour_6"),
      sum("web_hour_7").alias("web_hour_7"),
      sum("web_hour_8").alias("web_hour_8"),
      sum("web_hour_9").alias("web_hour_9"),
      sum("web_hour_10").alias("web_hour_10"),
      sum("web_hour_11").alias("web_hour_11"),
      sum("web_hour_12").alias("web_hour_12"),
      sum("web_hour_13").alias("web_hour_13"),
      sum("web_hour_14").alias("web_hour_14"),
      sum("web_hour_15").alias("web_hour_15"),
      sum("web_hour_16").alias("web_hour_16"),
      sum("web_hour_17").alias("web_hour_17"),
      sum("web_hour_18").alias("web_hour_18"),
      sum("web_hour_19").alias("web_hour_19"),
      sum("web_hour_20").alias("web_hour_20"),
      sum("web_hour_21").alias("web_hour_21"),
      sum("web_hour_22").alias("web_hour_22"),
      sum("web_hour_23").alias("web_hour_23"),
      sum("web_work_hours").alias("web_work_hours"),
      sum("web_evening_hours").alias("web_evening_hours"),
      max("cnt_uid").alias("cnt_uid"))
      .withColumn("web_fraction_work_hours", col("web_work_hours") / col("cnt_uid"))
      .withColumn("web_fraction_evening_hours", col("web_evening_hours") / col("cnt_uid"))
      .drop("web_work_hours","web_evening_hours","cnt_uid")

    val res = usersItems.join(result, Seq("uid"), "left")
    val res_2 = res.join(uid_data_transform_agg, Seq("uid"), "left")
    res_2.write.mode("overwrite").parquet("features")

  }
}
