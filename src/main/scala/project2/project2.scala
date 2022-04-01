package project2

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

    val coto = spark.read.option("header","true").csv(System.getProperty("user.dir")+"\\data\\covid_19_data.csv")
    coto.createOrReplaceTempView("covid19data")

    //testing my menu
    Menus.MainMenu(spark)

    //spark.read.csv(System.getProperty("user.dir")+"\\data\\covid_19_data.csv")
  }
  //Sets up the spark environment
}
