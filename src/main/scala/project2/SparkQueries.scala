package project2

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame

class SparkQueries(spark:SparkSession) {
  import spark.implicits._
  //Create your querries in separate functions like this
  //Alternatively name them like mynameQuery1,mynameQuery2 ect...
  //Or just whatever type of analysis you're doing like AverageConfirmed or whatever
  def ExampleQuery(): DataFrame={
    val df = Seq(("bob",2),("bill",4)).toDF("name","id")
    df
  }
  //ect...
}

