import org.apache.spark.sql.SparkSession

object project2 extends App {
  //Sets up the spark environment
  System.setProperty("hadoop.home.dir", "C:\\hadoop")
  val spark = SparkSession
    .builder
    .appName("IGDB query")
    .config("spark.master", "local[*]")
    .enableHiveSupport()
    .getOrCreate()
  println("Created Spark session\n")
  spark.sparkContext.setLogLevel("ERROR")
}
