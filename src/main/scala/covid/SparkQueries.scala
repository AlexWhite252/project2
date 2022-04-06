package project2

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame

class SparkQueries(spark:SparkSession) {

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
  def ExampleQuery(): DataFrame={
    val df = Seq(("bob",2),("bill",4)).toDF("name","id")
    df
  }
  //ect...
  def confirmedFirst(): Unit = {
    /*---Confirmed cases, deaths, recovered within FIRST 4 months---*/
    /*---Might need some tweaking because I get multiple countries per month but I have this so far---*/
    /*-- Reid: If I remembered wrong and the confirmed,deaths, and recovered aren't the amount  for that date
     * -- but an aggregation of all the numbers before it ie: day 1 and day 2 each had only 1 death
     * -- but on day 2 it shows 2 in deaths instead of 1
     * -- if that's the case change the SUM to MAX instead*/
    val df =spark.sql("SELECT DISTINCT " +
      "MONTH(from_unixtime(unix_timestamp(ObservationDate,'MM/dd/yyyy'))) AS Month_Number, " +
      "Country_Region AS Country,SUM(Confirmed) as Confirmed,SUM(Deaths) as Deaths,SUM(Recovered) as Recovered " +
      "FROM covid19data " +
      "WHERE ObservationDate BETWEEN '01/22/2020' AND '04/30/2020' " +
      "GROUP BY Month_Number,Country " +
      "ORDER BY Month_Number").toDF()
    //DFWriter.Write(path,df)
  }
  def confirmedLast(): Unit = {
    /*---Confirmed cases, deaths, recovered within LAST 4 months---*/
    /*---Might need some tweaking because I get multiple countries per month but I have this so far---*/
    /*-- Reid: If I remembered wrong and the confirmed,deaths, and recovered aren't the amount  for that date
    * -- but an aggregation of all the numbers before it ie: day 1 and day 2 each had only 1 death
    * -- but on day 2 it shows 2 in deaths instead of 1
    * -- if that's the case change the SUM to MAX instead*/
    val df=spark.sql("SELECT DISTINCT " +
      "MONTH(from_unixtime(unix_timestamp(ObservationDate,'MM/dd/yyyy'))) AS Month_Number, " +
      "Country_Region AS Country,SUM(Confirmed) as Confirmed,SUM(Deaths) as Deaths,SUM(Recovered) as Recovered " +
      "FROM covid19data " +
      "WHERE ObservationDate BETWEEN '02/02/2021' AND '05/02/2021' " +
      "GROUP BY Month_Number,Country " +
      "ORDER BY Month_Number").toDF()
    //DFWriter.Write(path,df)
  }
  def confirmedChina(): Unit = {
    /*---Confirmed in China then other countries---*/
    /*---Do we need SUM()?? with the way the data is formatted..---*/
    spark.sql("SELECT DISTINCT Country_Region AS Country,SUM(Confirmed) AS Confirmed from covid19data where Country_Region == 'Mainland China' AND Last_Update='2021-05-03 04:20:39' GROUP BY Country UNION SELECT DISTINCT Country_Region as Country,SUM(Confirmed)as not from covid19data where Country_Region != 'Mainland China' AND Last_Update='2021-05-03 04:20:39' GROUP BY Country").write.option("delimiter", ",").option("header", "true").option("inferSchema", "true").mode("overwrite").csv("src/main/Data/ConfirmedChina")
    /*---Confirmed in China w/ country count confirmed---*/
    spark.sql("SELECT DISTINCT Country_Region AS Country,SUM(Confirmed) AS Confirmed from covid19data where Country_Region == 'Mainland China' AND Last_Update='2021-05-03 04:20:39' GROUP BY Country UNION SELECT DISTINCT COUNT(Country_Region) as Country,SUM(Confirmed)as not from covid19data where Country_Region != 'Mainland China' AND Last_Update='2021-05-03 04:20:39'").write.option("delimiter", ",").option("header", "true").option("inferSchema", "true").mode("overwrite").csv("src/main/Data/ConfirmedChinaVsCount")
    /*---Sample stat---*/
    val countries = spark.sql("SELECT DISTINCT COUNT(Country_Region) FROM covid19data WHERE Country_Region!='%China%'").head().getLong(0)
    val confirmed = spark.sql("SELECT SUM(Confirmed) FROM covid19data WHERE Last_Update='2021-05-03 04:20:39'").head().getDouble(0)
    val chinaCases = 1 - ((countries / confirmed) * 100)
    println(f"China had ${chinaCases}%.0f%% of worldwide confirmed cases")
  }
  def topRecovered(): Unit = {
    /*---Top 10 recovered across countries---*/
    /*---Might need to alter due to large numbers (might not need SUM())?---*/
    spark.sql("SELECT DISTINCT Country_Region AS Country,SUM(Recovered) AS Recovered FROM covid19data WHERE Last_Update='2021-05-03 04:20:39' GROUP BY Country_Region ORDER BY Recovered DESC LIMIT 10").write.option("delimiter", ",").option("header", "true").option("inferSchema", "true").mode("overwrite").csv("data/TopRecovered")
  }
  def bottomRecovered(): Unit = {
    /*---Bottom 10 recovered across countries---*/
    spark.sql("SELECT DISTINCT Country_Region AS Country,SUM(Recovered) AS Recovered FROM covid19data WHERE Recovered>0 AND Last_Update='2021-05-03 04:20:39' GROUP BY Country_Region ORDER BY Recovered ASC LIMIT 10").write.option("delimiter", ",").option("header", "true").option("inferSchema", "true").mode("overwrite").csv("data/BottomRecovered")
  }
  def topDeaths(): Unit = {
    /*---Top 10 deaths across countries---*/
    spark.sql("SELECT DISTINCT Country_Region AS Country,SUM(Deaths) AS Deaths FROM covid19data WHERE Last_Update='2021-05-03 04:20:39' GROUP BY Country_Region ORDER BY Deaths DESC LIMIT 10").write.option("delimiter", ",").option("header", "true").option("inferSchema", "true").mode("overwrite").csv("data/TopDeaths")
  }
  def bottomDeaths(): Unit = {
    /*---Bottom 10 deaths across countries---*/
    spark.sql("SELECT DISTINCT Country_Region AS Country,SUM(Deaths) AS Deaths FROM covid19data WHERE Deaths>0 AND Last_Update='2021-05-03 04:20:39' GROUP BY Country_Region ORDER BY Deaths ASC LIMIT 10").write.option("delimiter", ",").option("header", "true").option("inferSchema", "true").mode("overwrite").csv("data/BottomDeaths")
  }
}

