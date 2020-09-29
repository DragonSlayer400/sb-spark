import org.apache.spark.sql.SparkSession

object KafkaMain {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    val kafkaParams = Map(
      "kafka.bootstrap.servers" -> "spark-master-1:6667",
      "subscribe" -> "lab04_input_data",
      "startingOffsets" -> """earliest"""
    )

    val sdf = spark.readStream.format("kafka").options(kafkaParams).load
    val parsedSdf = sdf.select('value.cast("string"), 'topic, 'partition, 'offset)

  }
}
