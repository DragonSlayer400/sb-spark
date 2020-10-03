import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Dataset

object filter {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._


    val offset = spark.sparkContext.getConf.get("spark.filter.offset")
    val df = spark.read.format("kafka").option("kafka.bootstrap.servers", "spark-master-1:6667").option("subscribe", spark.sparkContext.getConf.get("spark.filter.topic_name")).option("failOnDataLoss","false").option("startingOffsets",if (offset == "earliest") s"earliest" else s""" {"${spark.sparkContext.getConf.get("spark.filter.topic_name")}": {"0":$offset}}""").load()

    df.show(10)

    val json: Dataset[String] = df.select(col("value").cast("string")).as[String]

    val getData = udf { (timestamp: Long) =>
      val format = new java.text.SimpleDateFormat("yyyyMMdd")
      format.format(timestamp)
    }
    val parsed = spark.read.json(json).select('category,'event_type,'item_id,'item_price,'timestamp, 'uid, getData('timestamp).as("date")).withColumn("date_rep",'date)




    //parsed.filter(col("event_type") === "view")
   //   .write.partitionBy("date_rep").mode("overwrite").json(spark.sparkContext.getConf.get("spark.filter.output_dir_prefix") + "/view/")
   // parsed.filter(col("event_type") === "buy")
    //  .write.partitionBy("date_rep").mode("overwrite").json(spark.sparkContext.getConf.get("spark.filter.output_dir_prefix") + "/buy/")

    spark.stop()

  }
}
