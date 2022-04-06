package covid

/*
  Made by: Caleb Reid
  Created on: 4/5/2022
  Last Updated: 4/5/2022
  Summary: Custom object to write dataframes to a single file instead of partitioned out
*/

import org.apache.spark.sql.DataFrame

import java.io.PrintWriter

object DFWriter{

  /*def Write(path: String, df: DataFrame): Unit = {
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
    val partitions = df.rdd.getNumPartitions
    println(partitions)
    for(x <- 0 until partitions){
      println(s"${x/partitions}%")
      val part = df.rdd.mapPartitionsWithIndex((idx, iter) => if(idx ==x) iter else Iterator())
      printer.println(part.collect.mkString("\n").replaceAll("\\[|\\]",""))

    }
    /*df.foreachPartition(x=>{
      val get = new PartialFunction[Row,String] {
        def apply(x: Row) = x.mkString("\n").replaceAll("\\[|\\]","")
        def isDefinedAt(x:Row) = x!= null
      }
      //val part = x.mkString("\n").replaceAll("\\[|\\]","")
      val part = x.collect(get)
      println(s"${num/count}%")
      printer.println(part)
    })*/
    println(s"Done!\n")
    printer.close()
    //file.close()
  }*/
  def Write(path:String, df: DataFrame): Unit={
    val printer = new PrintWriter(path+".csv")
    println(s"Saving results to $path...")
    printer.println(df.columns.mkString(","))
    printer.println(df.collect.mkString("\n").replaceAll("\\[|\\]",""))
    printer.close()
    println("Done!")
  }


}
