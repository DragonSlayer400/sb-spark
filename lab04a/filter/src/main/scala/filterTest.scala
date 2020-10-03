import org.apache.spark.SparkContext
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
object filterTest extends App{
  val spark: SparkSession = SparkSession.builder()
    .appName(" Logs Kafka2Spark (Romanov Andrey)")
    .getOrCreate()
  val sc: SparkContext = spark.sparkContext
  val topicName: String = spark.conf.get("spark.filter.topic_name")
  val inputOffset: String = spark.conf.get("spark.filter.offset")
  val offsetType: String = if (inputOffset == "earliest"){
    s"earliest"
  } else{
    s""" {"$topicName": {"0":$inputOffset}}"""
  }


  println("OFFSET" + offsetType)

  val outputDirPrefix: String = spark.conf.get("spark.filter.output_dir_prefix")
  val kafkaParams: Map[String, String] = Map(
    "kafka.bootstrap.servers" -> "spark-master-1:6667",
    "subscribe" -> topicName,
    "failOnDataLoss" -> "false",
    "startingOffsets" -> offsetType
  )
  val schema = new StructType()
    .add("event_type", DataTypes.StringType, nullable = true)
    .add("category", DataTypes.StringType, nullable = true)
    .add("item_id", DataTypes.StringType, nullable = true)
    .add("item_price", DataTypes.IntegerType, nullable = true)
    .add("uid", DataTypes.StringType, nullable = true)
    .add("timestamp", DataTypes.LongType, nullable = true)
  val rawDF: DataFrame = spark.read
    .format("kafka")
    .options(kafkaParams)
    .load
  val rawStringDF: DataFrame = rawDF.selectExpr("CAST(value AS STRING)")
  val rawDataDF: DataFrame = rawStringDF
    .select(from_json(col("value"), schema).as("data"))
    .select("data.*")
  val rawDataChangedDF: DataFrame =
    rawDataDF.withColumn("date", from_unixtime(col("timestamp") / 1000, "yyyyMMdd"))
      .withColumn("part_date", from_unixtime(col("timestamp") / 1000, "yyyyMMdd"))
  rawDataChangedDF.show(10)
  val buyDataDF: DataFrame = rawDataChangedDF.filter(col("event_type") === "buy")
  val viewDataDF: DataFrame = rawDataChangedDF.filter(col("event_type") === "view")
  buyDataDF.write
    .format("json")
    .partitionBy("part_date")
    .json(s"$outputDirPrefix/buy")
  viewDataDF.write
    .format("json")
    .partitionBy("part_date").
    json(s"$outputDirPrefix/view")
  spark.stop()
}
