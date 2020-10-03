import org.apache.hadoop.util.Timer
import org.apache.spark.sql.catalyst.dsl.expressions.DslSymbol
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataTypes, StructType}
object agg {
  val spark = SparkSession.builder().getOrCreate()
  def main(args: Array[String]): Unit = {

    val df = spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers", "spark-master-1:6667")
      .option("subscribe", "lab04b_input_data")
      .option("startingOffsets", "earliest")
      .option("maxOffsetsPerTrigger","5000").load()

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
      .select(('timestamp/1000).cast("timestamp").as("timestamp"),'event_type,'category,'item_id,'item_price,'uid)
      .dropDuplicates(Seq("timestamp"))
      .withWatermark("timestamp", "1 hour")
      .groupBy(window($"timestamp", "1 hour","1 hour"))
      .agg(sum(when('event_type === "buy",col("item_price"))).as("revenue"), count(col("event_type").like("buy")).as("purchases"),count(col("uid").isNotNull).as("visitors"))
      .select(col("window.start").cast("long").as("start_ts"),col("window.end").cast("long").as("end_ts"),'revenue,'visitors,'purchases, ('revenue.cast("double")/'purchases.cast("double")).as("aov"))
      .toJSON
      .writeStream
      //.trigger(Trigger.ProcessingTime("5 seconds"))
      //.outputMode("update")
      .trigger(Trigger.Once())
      .outputMode("complete")
      .format("kafka")
      //.option("checkpointLocation", s"chk/$chkName")
      .option("kafka.bootstrap.servers","spark-master-1:6667")
      .option("topic","denis_nurdinov_lab04b_out")
      .start().awaitTermination()
  }
}
