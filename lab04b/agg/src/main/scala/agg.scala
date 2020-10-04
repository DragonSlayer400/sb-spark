import org.apache.hadoop.util.Timer
import org.apache.spark.sql.catalyst.dsl.expressions.DslSymbol
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataTypes, StructType, TimestampType}
object agg {
  val spark = SparkSession.builder().getOrCreate()
  def main(args: Array[String]): Unit = {

    val df = spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers", "spark-master-1:6667")
      .option("subscribe", "denis_nurdinov")
      .option("startingOffsets", "earliest")
      .option("maxOffsetsPerTrigger","5000")
      .option("failOnDataLoss","false")
      .load()

    saveLogs("lab04b",df)


    spark.close()

  }

  def saveLogs(chkName: String, df: DataFrame) = {
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
      .select(
        ('timestamp/1000).cast(TimestampType).as("timestamp"),
        'event_type,
        'category,
        'item_id,
        'item_price,
        'uid
      )
      .withWatermark("timestamp", "1 hour")
      .groupBy(window($"timestamp", "1 hour","1 hour"))
      .agg(
          sum(when('event_type === "buy",
          col("item_price"))).as("revenue"),
          count(when('event_type === "buy",true)).as("purchases"),
          count(when(col("uid").isNotNull,true)).as("visitors")
      )
      .withColumn("aov",'revenue/'purchases)
      .select(
        unix_timestamp(col("window.start")).as("start_ts"),
        unix_timestamp(col("window.end")).as("end_ts"),
        'revenue,
        'visitors,
        'purchases,
        'aov
      )
      .toJSON
      .writeStream
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .format("kafka")
      .option("checkpointLocation", s"chk/$chkName")
      .option("kafka.bootstrap.servers","spark-master-1:6667")
      .option("topic","denis_nurdinov_lab04b_out")
      .outputMode(OutputMode.Update())
      .start().awaitTermination()
  }
}
