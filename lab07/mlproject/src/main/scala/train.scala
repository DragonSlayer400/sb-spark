import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{CountVectorizer, StringIndexer, IndexToString}
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.functions._
object train {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().getOrCreate()
    val pathTraining = spark.sparkContext.getConf.get("spark.train.input_path")
    val pathModel = spark.sparkContext.getConf.get("spark.train.model_output")
    import spark.implicits._
    val training = spark.read.json(pathTraining)
      .select(col("uid"),col("gender_age"),explode(col("visits")))
      .select(col("uid"),col("gender_age"),col("col.*"))
      .withColumn("host", lower(callUDF("parse_url", $"url", lit("HOST"))))
      .withColumn("domains", regexp_replace($"host", "www.", "")).select(col("uid"),col("gender_age"),col("domains"))
      .groupBy(col("uid"),col("gender_age")).agg(collect_list("domains").as("domains"))
    val cv = new CountVectorizer()
      .setInputCol("domains")
      .setOutputCol("features")
    val indexer = new StringIndexer()
      .setInputCol("gender_age")
      .setOutputCol("label")
      .fit(training)
    val lr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.001)
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setPredictionCol("prediction")
    val labelConverter: IndexToString = new IndexToString().
      setInputCol("prediction").
      setOutputCol("predict_gender_age").
      setLabels(indexer.labels)
    val pipeline = new Pipeline()
      .setStages(Array(cv, indexer, lr, labelConverter))
    val model = pipeline.fit(training)
    model.write.overwrite().save(pathModel)
    spark.close()
  }

}
