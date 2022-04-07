

package project2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object project2{

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\hadoop")
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession

      .builder
      .appName("COVID data query")
      .config("spark.master", "local[*]")
      .enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    println("Created Spark session\n")




    //testing my menu
    Menus.MainMenu(spark)



  }
  //Sets up the spark environment
}
