package covid.FastDataAnalysis



import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Testmain {


  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    System.setProperty("hadoop.home.dir", "C:\\hadoop")
    val spark = SparkSession
      .builder
      .appName("IGDB query")
      .config("spark.master", "local[*]")
      .enableHiveSupport()
      .getOrCreate()
    println("Created Spark session\n")
    spark.sparkContext.setLogLevel("ERROR")



    var c = new ComparisonMenu

    c.ComparisonMenu.compInit()

  }

}



