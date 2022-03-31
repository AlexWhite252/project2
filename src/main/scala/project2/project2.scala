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

    val sparkqueries = new SparkQueries(spark)
    sparkqueries.Querry1()
    sparkqueries.Querry2()

  }
  //Sets up the spark environment
}
