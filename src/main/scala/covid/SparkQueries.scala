package covid

import covid.DFWriter
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame

class SparkQueries(spark:SparkSession) {

  val covid: DataFrame =spark.read.format("csv")
    .option("delimiter", ",")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("data/covid_19_data.csv").select(
    col("SNo"),col("Province_State"),col("Country_Region"),col("Confirmed"),
    col("Deaths"),col("Recovered"),
    to_date(col("ObservationDate"),"MM/dd/yyyy").as("ObservationDate"))
  covid.createOrReplaceTempView("covid19data")

  import spark.implicits._
  //Create your querries in separate functions like this
  //Alternatively name them like mynameQuery1,mynameQuery2 ect...
  //Or just whatever type of analysis you're doing like AverageConfirmed or whatever
  def ExampleQuery(): DataFrame={
    val df = Seq(("bob",2),("bill",4)).toDF("name","id")
    df
  }
  //ect...
  def confirmedFirst(): Unit = {
    /*---Confirmed cases, deaths, recovered within FIRST 4 months---*/
    val df =spark.sql("SELECT " +
      "ObservationDate AS Date, " +
      "Country_Region AS Country,SUM(Confirmed) as Confirmed,SUM(Deaths) as Deaths,SUM(Recovered) as Recovered " +
      "FROM covid19data " +
      "WHERE ObservationDate BETWEEN '2020-01-22' AND '2020-04-30' " +
      "GROUP BY Date,Country " +
      "ORDER BY Date").toDF()
    DFWriter.Write("data/First",df)
  }
  def confirmedLast(): Unit = {
    /*---Confirmed cases, deaths, recovered within LAST 4 months---*/
    val df=spark.sql("SELECT DISTINCT " +
      "ObservationDate AS Date, " +
      "Country_Region AS Country,SUM(Confirmed) as Confirmed,SUM(Deaths) as Deaths,SUM(Recovered) as Recovered " +
      "FROM covid19data " +
      "WHERE ObservationDate BETWEEN '2021-02-02' AND '2021-05-02' " +
      "GROUP BY Date,Country " +
      "ORDER BY Date").toDF()
    DFWriter.Write("data/Last",df)
  }
  def Overview(): Unit={
    val df =spark.sql("SELECT DISTINCT " +
      "MONTH(from_unixtime(unix_timestamp(ObservationDate,'MM/dd/yyyy'))) AS Month_Number, " +
      "Country_Region AS Country,MAX(Confirmed) as Confirmed,MAX(Deaths) as Deaths,MAX(Recovered) as Recovered " +
      "FROM covid19data " +
      "WHERE ObservationDate BETWEEN '01/22/2020' AND '12/30/2020' " +
      "GROUP BY Month_Number,Country " +
      "ORDER BY Month_Number").toDF()
    DFWriter.Write("data/Year",df)
  }
  def confirmedChina(): Unit = {
    /*---Confirmed in China then other countries---*/
    spark.sql("SELECT DISTINCT " +
      "Country_Region AS Country,MAX(Confirmed) AS Confirmed " +
      "FROM covid19data " +
      "WHERE Country_Region == 'Mainland China' AND Last_Update='2021-05-03 04:20:39' " +
      "GROUP BY Country " +
      "UNION SELECT DISTINCT " +
      "Country_Region as Country,MAX(Confirmed) " +
      "FROM covid19data " +
      "WHERE Country_Region != 'Mainland China' AND Last_Update='2021-05-03 04:20:39' " +
      "GROUP BY Country")

    /*---Confirmed in China w/ country count confirmed---*/
    spark.sql("SELECT DISTINCT " +
      "Country_Region AS Country,MAX(Confirmed) AS Confirmed " +
      "FROM covid19data " +
      "WHERE Country_Region == 'Mainland China' AND Last_Update='2021-05-03 04:20:39' " +
      "GROUP BY Country " +
      "UNION SELECT DISTINCT " +
      "COUNT(Country_Region) as Country,MAX(Confirmed) " +
      "FROM covid19data " +
      "WHERE Country_Region != 'Mainland China' AND Last_Update='2021-05-03 04:20:39'")

    /*---Sample stat---*/
    val countries = spark.sql("SELECT DISTINCT COUNT(Country_Region) FROM covid19data WHERE Country_Region!='%China%'").head().getLong(0)
    val confirmed = spark.sql("SELECT SUM(Confirmed) FROM covid19data WHERE Last_Update='2021-05-03 04:20:39'").head().getDouble(0)
    val chinaCases = 1 - ((countries / confirmed) * 100)
    //println(f"China had ${chinaCases}%.0f%% of worldwide confirmed cases")
  }
  def topRecovered(): Unit = {
    /*---Top 10 recovered across countries---*/
    spark.sql("SELECT DISTINCT " +
      "Country_Region AS Country,MAX(Recovered) AS Recovered " +
      "FROM covid19data " +
      "GROUP BY Country " +
      "ORDER BY Recovered DESC LIMIT 10").toDF()
    DFWriter.Write("data/topRecovered",df)
  }
  def bottomRecovered(): Unit = {
    /*---Bottom 10 recovered across countries---*/
    spark.sql("SELECT DISTINCT " +
      "Country_Region AS Country,MAX(Recovered) AS Recovered " +
      "FROM covid19data " +
      "WHERE Recovered>0 " +
      "GROUP BY Country " +
      "ORDER BY Recovered ASC LIMIT 10").toDF()
    DFWriter.Write("data/bottomRecovered",df)
  }
  def topDeaths(): Unit = {
    /*---Top 10 deaths across countries---*/
    spark.sql("SELECT DISTINCT " +
      "Country_Region AS Country,MAX(Deaths) AS Deaths " +
      "FROM covid19data " +
      "GROUP BY Country " +
      "ORDER BY Deaths DESC LIMIT 10").toDF()
    DFWriter.Write("data/topDeaths",df)
  }
  def bottomDeaths(): Unit = {
    /*---Bottom 10 deaths across countries---*/
    spark.sql("SELECT DISTINCT " +
      "Country_Region AS Country,MAX(Deaths) AS Deaths " +
      "FROM covid19data " +
      "WHERE Deaths>0 " +
      "GROUP BY Country " +
      "ORDER BY Deaths ASC LIMIT 10").toDF()
    DFWriter.Write("data/bottomDeaths",df)
  }
}

