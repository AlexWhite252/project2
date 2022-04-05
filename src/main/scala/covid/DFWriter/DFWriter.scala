package covid.DFWriter

/*
  Made by: Caleb Reid
  Created on: 4/5/2022
  Last Updated: 4/5/2022
  Summary: Custom object to write dataframes to a single file instead of partitioned out
*/

import org.apache.spark.sql.DataFrame

import java.io.PrintWriter

class DFWriter extends java.io.Serializable{

  def Write(path: String, df: DataFrame): Unit = {
    //val file = new FileOutputStream(path+".csv")
    //val printer = new ObjectOutputStream(file)
    //val printer = new PrintStream(path+".csv")
    val printer = new PrintWriter(path + ".csv")
    //val printer = new PrintWriterSerialize(path+".csv")
    printer.println(df.columns.mkString(","))
    println(s"Beginning to write to file $path...")
    //partData.foreachPartition(x=>println(x.mkString("\n").replaceAll("\\[|\\]","")))
    var num = 0
    val count = df.count().toInt
    df.foreachPartition(x=>{
      val part = x.mkString("\n").replaceAll("\\[|\\]","")
      println(s"${num/count}%")
      printer.println(part)
    })
    println(s"Done!\n")
    printer.close()
    //file.close()
  }


}
