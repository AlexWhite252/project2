package project2

import org.apache.spark.sql.SparkSession

object project2{
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\hadoop")
    val spark = SparkSession
      .builder
      .appName("IGDB query")
      .config("spark.master", "local[*]")
      .enableHiveSupport()
      .getOrCreate()
    println("Created Spark session\n")
    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    val a = Seq(("hawaii", 50), ("brazil", 30)).toDF()
    a.show()
  }
  //Sets up the spark environment
}
