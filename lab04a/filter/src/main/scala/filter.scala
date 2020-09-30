import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Dataset

object filter {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._


    val offset = spark.sparkContext.getConf.get("spark.filter.offset")
    val df = spark.read.format("kafka").option("kafka.bootstrap.servers", "spark-master-1:6667").option("subscribe", "lab04_input_data").option("startingOffsets",if (offset == "earliest") s"$offset" else s""" { "lab04_input_data": {"0": $offset } """).option("failOnDataLoss","false").load()
    spark.sparkContext.getConf.getAll.foreach(x => println("key: " + x._1 + " value: " + x._2))

    df.show(10)


    println("count " + df.count())
    val json: Dataset[String] = df.select(col("value").cast("string")).as[String]

    val getData = udf { (timestamp: Long) =>
      val format = new java.text.SimpleDateFormat("yyyyMMdd")
      format.format(timestamp)
    }
    val parsed = spark.read.json(json).select('category,'event_type,'item_id,'item_price,'timestamp, 'uid, getData('timestamp).as("date")).withColumn("date_rep",'date)




    parsed.filter(col("event_type") === "view")
      .write.partitionBy("date_rep").mode("overwrite").json(spark.sparkContext.getConf.get("spark.filter.output_dir_prefix") + "/view/")
    parsed.filter(col("event_type") === "buy")
      .write.partitionBy("date_rep").mode("overwrite").json(spark.sparkContext.getConf.get("spark.filter.output_dir_prefix") + "/buy/")

    spark.stop()

  }
}
