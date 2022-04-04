class DataTableBuilder {



  object dataTableCovid
{

  var spark = new Sparker

  spark.spark().sql("")


  def Data_Table(textFile:String):Unit =
    {
      spark.spark().sql("Drop table if exists Covid_19_Data")

      spark.spark().sql("CREATE TABLE Covid_19_Data (Sno Int, ObservationDate Date,Province_State String " +
        "NOT NULL, Country String, Last_Update date,Confirmed Int, Deaths Int, Recovered Int)row format delimited " +
        "fields terminated by ',' stored as textfile").show(false)

      spark.spark().sql(s"LOAD DATA LOCAL INPATH '$textFile' " +
        "INTO TABLE Covid_19_Data").show(100000,100000, false)

      spark.spark().sql("Select * from Covid_19_Data")
    }

def Death_Table(textFile:String):Unit=
  {
    spark.spark().sql("Drop table if exists Covid_19_deaths")
    spark.spark().sql("Create table Covid_19_deaths (State_Province String, Region String, Lat Double, Long Double) row format delimited fields terminated by ',' stored as textfile").show(false)
    spark.spark().sql("LOAD DATA LOCAL INPATH 'time_series_covid_19_deaths.csv' INTO TABLE Covid_19_deaths").show(100000,100000, false)

  }






}












}
