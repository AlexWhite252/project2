package covid

import covid.DFWriter.DFWriter
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructField, StructType}

class SparkQueries(spark:SparkSession) {
//SNo,ObservationDate,Province_State,Country_Region,Last_Update,Confirmed,Deaths,Recovered
  val covidSchema: StructType = StructType(Array(
    StructField("SNo",IntegerType,true),
    StructField("ObservationDate",DateType,true),
    StructField("Province_State",StringType,true),
    StructField("Country_Region",StringType,true),
    StructField("Last_Update",DateType,true),
    StructField("Confirmed",IntegerType,true),
    StructField("Deaths",IntegerType,true),
    StructField("Recovered",IntegerType,true)
  ))
  val covid: DataFrame =spark.read.format("csv")
    .option("delimiter", ",")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("data/covid_19_data.csv")
  covid.createOrReplaceTempView("covid19data")

  import spark.implicits._
  //Create your querries in separate functions like this
  //Alternatively name them like mynameQuery1,mynameQuery2 ect...
  //Or just whatever type of analysis you're doing like AverageConfirmed or whatever
  def AllQuery(): Unit={
    confirmedLast()
    confirmedFirst()
    topRecovered()
    topDeaths()
    bottomDeaths()
    bottomRecovered()
  }
  def ExampleQuery(): DataFrame={
    val df = Seq(("bob",2),("bill",4)).toDF("name","id")
    df
  }
  //ect...
  def confirmedFirst(): Unit = {
    /*---Confirmed cases, deaths, recovered within FIRST 4 months---*/
    val data: DataFrame = spark.sql(
      "SELECT Country_Region AS Country,SUM(Confirmed) AS Confirmed_Cases,SUM(Deaths) AS Deaths,SUM(Recovered) AS Recovered, " +
        "ObservationDate as Month " +
        "FROM covid19data " +
        "WHERE ObservationDate BETWEEN '01/22/2020' AND '05/22/2020' " +
        "GROUP BY Country,Month").write.
    val partData = data.repartition($"Month")
    //partData.foreachPartition(x=>println(x.mkString("\n").replaceAll("\\[|\\]","")))
    val writer = new DFWriter()
    writer.Write("data/First",partData)

  }
  def confirmedLast(): Unit = {
    /*---Confirmed cases, deaths, recovered within LAST 4 months---*/
    spark.sql(
      "SELECT Country_Region AS Country,SUM(Confirmed) AS Confirmed_Cases,SUM(Deaths) AS Deaths,SUM(Recovered) AS Recovered, " +
        "MONTH(ObservationDate) as Month " +
        "FROM covid19data " +
        "WHERE ObservationDate BETWEEN '01/02/2021' AND '05/02/2021' " +
        "GROUP BY Country, Month")
  }
  def confirmedChina(): Unit = {
    /*---Confirmed in China vs rest of the world---*/
    spark.sql("SELECT DISTINCT Country_Region AS Country,SUM(Confirmed) AS Confirmed from covid19data where Country_Region == 'Mainland China' AND Last_Update='2021-05-03 04:20:39' GROUP BY Country UNION SELECT DISTINCT Country_Region as Country,SUM(Confirmed)as not from covid19data where Country_Region != 'Mainland China' AND Last_Update='2021-05-03 04:20:39' GROUP BY Country").write.option("delimiter", ",").option("header", "true").option("inferSchema", "true").mode("overwrite").csv("src/main/Data/ConfirmedChina")
    spark.sql("SELECT DISTINCT Country_Region AS Country,SUM(Confirmed) AS Confirmed from covid19data where Country_Region == 'Mainland China' AND Last_Update='2021-05-03 04:20:39' GROUP BY Country UNION SELECT DISTINCT COUNT(Country_Region) as Country,SUM(Confirmed)as not from covid19data where Country_Region != 'Mainland China' AND Last_Update='2021-05-03 04:20:39'").write.option("delimiter", ",").option("header", "true").option("inferSchema", "true").mode("overwrite").csv("src/main/Data/ConfirmedChinaVsCount")

    val countries = spark.sql("SELECT DISTINCT COUNT(Country_Region) FROM covid19data WHERE Country_Region!='%China%'").head().getLong(0)
    val confirmed = spark.sql("SELECT SUM(Confirmed) FROM covid19data WHERE Last_Update='2021-05-03 04:20:39'").head().getDouble(0)
    val chinaCases = 1 - ((countries / confirmed) * 100)
    println(f"China had ${chinaCases}%.0f%% of worldwide confirmed cases")
  }
  def topRecovered(): Unit = {
    /*---Top 10 recovered across countries---*/
    spark.sql(
      "SELECT DISTINCT Country_Region AS Country,SUM(Recovered) AS Recovered FROM covid19data " +
        "GROUP BY Country_Region " +
        "ORDER BY Recovered DESC LIMIT 10")
  }
  def bottomRecovered(): Unit = {
    /*---Bottom 10 recovered across countries---*/
    spark.sql(
      "SELECT DISTINCT Country_Region AS Country,SUM(Recovered) AS Recovered FROM covid19data " +
        "WHERE Recovered>0 " +
        "GROUP BY Country_Region ORDER BY Recovered ASC LIMIT 10")
  }
  def topDeaths(): Unit = {
    /*---Top 10 deaths across countries---*/
    spark.sql(
      "SELECT DISTINCT Country_Region AS Country,SUM(Deaths) AS Deaths FROM covid19data " +
      "GROUP BY Country_Region " +
      "ORDER BY Deaths DESC LIMIT 10")
  }
  def bottomDeaths(): Unit = {
    /*---Bottom 10 deaths across countries---*/
    spark.sql(
      "SELECT DISTINCT Country_Region AS Country,SUM(Deaths) AS Deaths FROM covid19data " +
        "WHERE Deaths>0 " +
        "GROUP BY Country_Region " +
        "ORDER BY Deaths ASC LIMIT 10")
  }
}

