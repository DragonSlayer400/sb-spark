import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import ru.apache.denis.VisitsProcessing
object users_items {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().getOrCreate()

    val inputDir = spark.sparkContext.getConf.get("spark.users_items.input_dir")

    val outputDir = spark.sparkContext.getConf.get("spark.users_items.output_dir")

    val updateMode = spark.sparkContext.getConf.get("spark.users_items.update");
    
    val visitsProcessing = new VisitsProcessing(spark, inputDir, outputDir)

    updateMode.toInt match {
      case 0 => visitsProcessing.update_off()
      case 1 => visitsProcessing.update_on()
    }

  }
}
