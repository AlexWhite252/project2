package covid

/*
  Made by: Caleb Reid
  Created on: 4/5/2022
  Last Updated: 4/5/2022
  Summary: Custom object to write dataframes to a single file instead of partitioned out
*/


import org.apache.spark.sql.{DataFrame, DataFrameWriter, Dataset, SaveMode}

import java.io.PrintWriter

/**
 * Saves a DataFrame to a single file instead of in multiple parts
 */

object DFWriter{

  /**
   * @param path  Desired filepath
   * @param df  DataFrame to be saved
   */
  def Write(path:String, df: DataFrame): Unit={
    val printer = new PrintWriter(path+".csv")
    println(s"Saving results to $path...")

    //Prints the column names to be used as headers
    printer.println(df.columns.mkString(","))

    //Collects the DataFrame and splits it by making a newline and removing the brackets
    printer.println(df.collect.mkString("\n").replaceAll("\\[|\\]",""))
    printer.close()
    println("Done!")
  }

}
