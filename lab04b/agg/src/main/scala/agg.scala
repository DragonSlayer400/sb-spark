import org.apache.hadoop.util.Timer
import org.apache.spark.sql.catalyst.dsl.expressions.DslSymbol
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataTypes, StructType, TimestampType}
object agg {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().getOrCreate()
    val df = spark.readStream.format("kafka")
      .options(
        Map(
          "kafka.bootstrap.servers" -> "spark-master-1:6667",
          "subscribe" -> "denis_nurdinov",
          "maxOffsetsPerTrigger" -> "5000",
          "startingOffsets" -> "earliest"
        )
      )
      .load()

    saveLogs("lab04b",df,spark).start()

    spark.streams.awaitAnyTermination()
    spark.close()

  }

  def saveLogs(chkName: String, df: DataFrame,spark: SparkSession) = {
    import spark.implicits._
    val schema = new StructType()
      .add("event_type", DataTypes.StringType, nullable = true)
      .add("category", DataTypes.StringType, nullable = true)
      .add("item_id", DataTypes.StringType, nullable = true)
      .add("item_price", DataTypes.IntegerType, nullable = true)
      .add("uid", DataTypes.StringType, nullable = true)
      .add("timestamp", DataTypes.LongType, nullable = true)
    df
      .select(from_json('value.cast("string"),schema).as("data"))
      .select("data.*")
      .withColumn("timestamp_cast", (col("timestamp")/1000).cast(TimestampType))
      .withWatermark("timestamp_cast","1 hour")
      .groupBy(window($"timestamp_cast", "1 hour","1 hour"))
      .agg(
        sum(when('event_type === "buy", col("item_price").as("item_price_agg"))).as("revenue"),
        count(when(col("event_type") === "buy",true)).as("purchases"),
        count(when(col("uid").isNotNull,true)).as("visitors")
      )
      .withColumn(
        "aov",
        col("revenue").cast(DataTypes.DoubleType)/col("purchases").cast(DataTypes.DoubleType)
      )
      .select(
        unix_timestamp(col("window.start")).as("start_ts"),
        unix_timestamp(col("window.end")).as("end_ts"),
        col("revenue"),
        col("visitors"),
        col("purchases"),
        col("aov")
      )
      .toJSON
      .writeStream
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .format("kafka")
      .options(
        Map(
          "kafka.bootstrap.servers" -> "spark-master-1:6667:6667",
          "topic" -> "denis_nurdinov_lab04b_out",
          "checkpointLocation" -> "chk/lab04b"
        )
      )
      .outputMode(OutputMode.Update())
  }
}
