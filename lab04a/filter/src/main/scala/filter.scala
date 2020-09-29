import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Dataset

object filter {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    val df = spark.read.format("kafka").option("kafka.bootstrap.servers", "spark-master-1:6667").option("subscribe", "lab04_input_data").load()

    df.show(10)

    val json: Dataset[String] = df.select(col("value").cast("string")).as[String]

    val getData = udf { (timestamp: Long) =>
      val format = new java.text.SimpleDateFormat("yyyyMMdd")
      format.format(timestamp)
    }
    val parsed = spark.read.json(json).select('category,'event_type,'item_id,'item_price,'timestamp, 'uid, getData('timestamp).as("date"))


    spark.sparkContext.getConf.getAll.foreach(x => println("key: " + x._1 + " value: " + x._2))


    parsed.filter(col("event_type") === "view").write.partitionBy("date").mode("overwrite").json(spark.sparkContext.getConf.get("spark.filter.output_dir_prefix") + "/view/")
    parsed.filter(col("event_type") === "buy").write.partitionBy("date").mode("overwrite").json(spark.sparkContext.getConf.get("spark.filter.output_dir_prefix") + "/buy/")

    spark.stop()

  }
}
