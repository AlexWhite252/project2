package project2.`FastDataAnalysis(WIP)`

import org.apache.spark.sql.DataFrame


class DataTableBuilder {



  object dataTableCovid
{

  var spark = new Sparker

  //spark.spark().sql("")


  def Data_Table():Unit =
    {
      spark.spark().sql("Drop table if exists covid19data")
      val covid: DataFrame =spark.spark().read.format("csv")
        .option("delimiter", ",")
        .option("header", "true")
        .option("inferSchema", "true")
        .load("covid_19_data.csv")
      covid.createOrReplaceTempView("covid19data")


      println("Working...")

      //spark.spark().sql("Select * from covid19data")
    }

def Death_Table(textFile:String):Unit=
  {
    spark.spark().sql("Drop table if exists Covid_19_deaths")
    spark.spark().sql("Create table Covid_19_deaths (State_Province String, Region String, Lat Double, Long Double) row format delimited fields terminated by ',' stored as textfile").show(false)
    spark.spark().sql("LOAD DATA LOCAL INPATH 'time_series_covid_19_deaths.csv' INTO TABLE Covid_19_deaths").show(100000,100000, false)

  }






}












}
