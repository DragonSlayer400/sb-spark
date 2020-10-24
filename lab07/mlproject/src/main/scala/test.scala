import org.apache.spark.SparkConf
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{ArrayType, DataTypes, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode.Update
import org.apache.spark.sql.streaming.Trigger

object test {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().getOrCreate()
    val output_model = spark.sparkContext.getConf.get("spark.test.output_model")
    val input_topic = spark.sparkContext.getConf.get("spark.test.input_topic")
    val output_topic = spark.sparkContext.getConf.get("spark.test.output_topic")
    val model = PipelineModel.load(output_model)
    val schema = StructType(
      List(
        StructField("uid", StringType),
        StructField("visits",
          ArrayType(
            StructType(
              List(
                StructField("url", StringType),
                StructField("timestamp", LongType)
              )
            )
          )
        )
      )
    )
    val logDf = spark.readStream
      .format("kafka")
      .options(
        Map(
          "kafka.bootstrap.servers" -> "spark-master-1:6667",
          "subscribe" -> input_topic,
          "maxOffsetsPerTrigger" -> "5000",
          "startingOffsets" -> "earliest"
        )
      )
      .load()
      .select(col("value").cast("String").as("data"))
      .withColumn("jsonCol", from_json(col("data"), schema))
      .select(col("jsonCol.*"))
      .select(col("uid"),explode(col("visits")))
      .select(col("uid"),col("col.*"))
      .withColumn("host", lower(callUDF("parse_url", col("url"), lit("HOST"))))
      .withColumn("domain", regexp_replace(col("host"), "www.", ""))
    val agg_log = logDf
      .groupBy("uid")
      .agg(collect_list("domain").alias("domains"))

    val agg_log_predict = model.transform(agg_log)

    agg_log_predict
      .select(col("uid"), col("predict_gender_age").alias("gender_age"))
      .toJSON
      .writeStream
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .format("kafka")
      .options(Map("kafka.bootstrap.servers" -> "spark-master-1:6667",
        "topic" -> output_topic,
        "checkpointLocation" -> "chk/lab07"))
      .outputMode(Update()).start()
    spark.streams.awaitAnyTermination()
    spark.close()
  }
}
