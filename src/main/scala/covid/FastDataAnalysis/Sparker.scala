package covid.FastDataAnalysis
import org.apache.spark.sql.SparkSession

class Sparker {
  def spark(): SparkSession={
      System.setProperty("hadoop.home.dir", "C:\\hadoop")
      val spark = SparkSession
        .builder
        .appName("IGDB query")
        .config("spark.master", "local[*]")
        .enableHiveSupport()
        .getOrCreate()
      //println("Created Spark session\n")
      spark.sparkContext.setLogLevel("ERROR")
      return spark
    }









}
