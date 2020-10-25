import org.apache.spark.SparkConf
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{callUDF, col, collect_list, explode, from_json, lit, lower, regexp_replace}
import org.apache.spark.sql.streaming.OutputMode.Update
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{ArrayType, LongType, StringType, StructField, StructType}

object dashboard {
  def main(args: Array[String]): Unit = {


    val esOpt = Map(
        "es.nodes" -> "10.0.0.5:9200",
        "es.batch.write.refresh" -> "false",
        "es.nodes.wan.only" -> "true",
        "es.resource" -> "denis_nurdinov_lab08/_doc"
      )

    //val sparkConf = new SparkConf().setAppName("lab08").setMaster("local")
    //val modelInput = "C:\\Users\\denis\\Desktop\\model"
    //val datasetInput = "C:\\Users\\denis\\Desktop\\laba08.json"

    val spark = SparkSession.builder()
      //.config(sparkConf)
      .getOrCreate()


    val datasetInput = spark.sparkContext.getConf.get("spark.test.dataset_input")
    val modelInput = spark.sparkContext.getConf.get("spark.dashboard.model_input")


    val model = PipelineModel.load(modelInput)

    val logDf = spark.read.json(datasetInput).select(col("date"),col("uid"),explode(col("visits")))
      .select(col("date"),col("uid"),col("col.url"))
      .withColumn("host", lower(callUDF("parse_url", col("url"), lit("HOST"))))
      .withColumn("domain", regexp_replace(col("host"), "www.", ""))
      .groupBy(col("uid"),col("date"))
      .agg(collect_list("domain").alias("domains"))

    val agg_log_predict = model.transform(logDf).select(col("uid"),col("date"),col("predict_gender_age").as("gender_age"))


    agg_log_predict.write.format("org.elasticsearch.spark.sql").options(esOpt).save()

    spark.close()
  }
}
