import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Dataset

object filter {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    val df = spark.read.format("kafka").option("kafka.bootstrap.servers", "spark-master-1:6667").option("subscribe", "lab04_input_data").load()
    val json: Dataset[String] = df.select(col("value").cast("string")).as[String]

    val getData = udf { (timestamp: Long) =>
      val format = new java.text.SimpleDateFormat("yyyyMMdd")
      format.format(timestamp)
    }
    val parsed = spark.read.json(json).select('category,'event_type,'item_id,'item_price,'timestamp, 'uid, getData('timestamp).as("date"))

    parsed.filter(col("event_type") === "view").write.partitionBy("date").mode("overwrite").json("/user/denis.nurdinov/visits/view/")
    parsed.filter(col("event_type") === "buy").write.partitionBy("date").mode("overwrite").json("/user/denis.nurdinov/visits/buy/")

    spark.stop()

  }
}
