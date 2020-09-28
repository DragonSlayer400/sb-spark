import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.{DataFrame, SparkSession}
import ru.apache.spark.lab03.cassandra.CassandraWork
import ru.apache.spark.lab03.elastic.Elastic
import ru.apache.spark.lab03.hdfs.HdfsWork
import ru.apache.spark.lab03.postgres.PostgresWork

object data_mart {

  val spark = SparkSession.builder().getOrCreate()

  var cassandraDataFrame: DataFrame = null

  var elasticDataFrame: DataFrame = null

  var postgresDataFrame: DataFrame = null

  var hdfsDataFrame: DataFrame = null

  def main(args: Array[String]): Unit = {
    initial()
    val ageCat = udf { (age: Int) =>
      age match {
        case x if (18 <= x && 24 >= x) => "18-24"
        case x if (25 <= x && 34 >= x) => "25-34"
        case x if (35 <= x && 44 >= x) => "35-44"
        case x if (45 <= x && 54 >= x) => "45-54"
        case x if (55 <= x) => ">=55"
      }
    }

    val dataFrameUser = cassandraDataFrame.withColumn("age_cat", ageCat(col("age"))).select(col("uid"), col("gender"), col("age_cat"))


    val shopCat = udf { (category: String) =>
      "shop_" + category.replaceAll(" ", "_").replaceAll("-", "_").toLowerCase
    }


    val webCat = udf { (category: String) =>
      "web_" + category.replaceAll(" ", "_").replaceAll("-", "_").toLowerCase
    }


    val elasticCatCount = elasticDataFrame.select(col("uid"), shopCat(col("category")).as("category")).groupBy("uid").pivot("category").count().na.fill(0)
    val logsWebJoinDomainCatCount = hdfsDataFrame.join(postgresDataFrame, Seq("domain"), "inner").select(col("uid"), webCat(col("category")).as("category")).groupBy("uid").pivot("category").count().na.fill(0)

    val countVisitsUser = dataFrameUser.join(elasticCatCount, Seq("uid"), "left").join(logsWebJoinDomainCatCount, Seq("uid"), "left")

    val postgresWork = new PostgresWork(spark)

    postgresWork.write(countVisitsUser, "jdbc:postgresql://10.0.0.5:5432/denis_nurdinov?user=denis_nurdinov&password=cwSXvVde")


    close()
  }


  def initial(): Unit = {
    spark.conf.set("spark.cassandra.connection.host", "10.0.0.5")
    spark.conf.set("spark.cassandra.connection.port", "9042")
    val cassandraWork = new CassandraWork(spark)
    val elasticWork = new Elastic(spark)
    val postgresWork = new PostgresWork(spark)
    val hdfsWork = new HdfsWork(spark)
    cassandraDataFrame = cassandraWork.read()
    elasticDataFrame = elasticWork.read()
    postgresDataFrame = postgresWork.read()
    hdfsDataFrame = hdfsWork.read()
  }


  def close(): Unit = {
    spark.close()
  }

}
