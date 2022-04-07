package covid
// Last updated 4/4/2022
import org.apache.spark.sql.SparkSession
object project2{

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\hadoop")
    val spark = SparkSession
      .builder
      .appName("COVID data query")
      .config("spark.master", "local[*]")
      .enableHiveSupport()
      .getOrCreate()
    println("Created Spark session\n")
    spark.sparkContext.setLogLevel("ERROR")
    val sq = new SparkQueries(spark)

  }
}
