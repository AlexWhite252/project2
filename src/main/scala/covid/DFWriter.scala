package covid
/*
  Made by: Caleb Reid
  Created on: 4/5/2022
  Last Updated: 4/5/2022
  Summary: Custom object to write dataframes to a single file instead of partitioned out
*/

import org.apache.spark.sql.DataFrame

import java.io.PrintWriter

object DFWriter {

  def Write(path: String, df: DataFrame): Unit = {
    val printer = new PrintWriter(path + ".csv")
    printer.println(df.columns.mkString(","))
    for (x <- 0 until df.count().toInt) {
      val row = df.rdd.zipWithIndex()
        .filter(j => j._2.toInt == x)
        .collect()
      printer.println(row.array(0)._1.mkString(","))
    }
    printer.close()
  }

}
