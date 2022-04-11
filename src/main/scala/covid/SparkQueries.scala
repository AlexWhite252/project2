package covid

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, to_date}

import scala.io.StdIn.readLine

/**
 * Main class for querying the data from the covid19 csv
 * @param spark The active spark session
 */

class SparkQueries(spark:SparkSession,dfw: DFWriter,path: String) {

  val save = (path:String, df:DataFrame) =>{
    println("Would you like to save the file?\n[csv]\n[json]\n[no]")
    readLine.toLowerCase match{
      case "csv"=> dfw.CSV(path,df)
      case "json"=> dfw.JSON(path,df)
      case _ =>
    }
  }

  val covid: DataFrame =spark.read.format("csv")
    .option("delimiter", ",")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(s"$path/covid_19_data.csv").select(
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
    df.show()
    save("First",df)
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
    df.show()
    save("Last",df)
  }
  def ChinaVsTheWorld(): Unit={
    val df =spark.sql(
      "SELECT 'China' AS Country, ObservationDate as Date, SUM(Confirmed) as Confirmed, " +
        "SUM(Deaths) AS Deaths,SUM(Recovered) AS Recovered " +
        "FROM covid19data " +
        "WHERE Country_Region='Mainland China' " +
        "GROUP BY Country,Date " +
        //"ORDER BY Date " +
        "UNION " +
        "SELECT 'World' AS Country, ObservationDate as Date, AVG(Confirmed) AS Confirmed, AVG(Deaths) as Deaths, " +
        "AVG(Recovered) as Recovered " +
        "FROM covid19Data " +
        "WHERE Country_Region!='Mainland China' " +
        "GROUP BY Country,Date " +
        "ORDER BY Date"
    )
    df.show()
    save("ChinaVsTheWorld",df)
  }
  def topRecovered(): Unit = {
    /*---Top 10 recovered across countries---*/
    /*---Might need to alter due to large numbers (might not need SUM())?---*/
    spark.sql(
      "SELECT DISTINCT Country_Region AS Country,MAX(Recovered) AS Recovered " +
        "FROM covid19data " +
        "GROUP BY Country_Region " +
        "ORDER BY Recovered DESC LIMIT 10").createOrReplaceTempView("topRecovered")
    val df=spark.sql(
      "SELECT Country_Region AS Country, ObservationDate as Date, SUM(cd.Recovered) AS Recovered FROM covid19data cd " +
        "JOIN topRecovered tr ON tr.Country=cd.Country_Region " +
        "GROUP BY cd.Country_Region,cd.ObservationDate " +
        "ORDER BY cd.ObservationDate"
    ).toDF()
    spark.sql("SELECT * FROM topRecovered").show()
    save("topRecovered",df)
  }
  def topConfirmed(): Unit={
    spark.sql(
      "SELECT Country_Region AS Country, MAX(Confirmed) AS Confirmed " +
        "FROM covid19data " +
        "GROUP BY Country_Region " +
        "ORDER BY Confirmed DESC LIMIT 10").createOrReplaceTempView("topConfirmed")
    val df = spark.sql(
      "SELECT Country_Region as Country, ObservationDate as Date, SUM(cd.Confirmed) AS Confirmed " +
        "FROM covid19data cd " +
        "JOIN topConfirmed tc ON tc.Country=cd.Country_Region " +
        "GROUP BY cd.Country_Region,cd.ObservationDate " +
        "ORDER BY ObservationDate"
    ).toDF()
    spark.sql("SELECT * FROM topConfirmed").show()
    save("topConfirmed",df)
  }
  def bottomConfirmed(): Unit={
    val df=spark.sql(
      "SELECT Country_Region AS Country, MAX(Confirmed) AS Confirmed " +
        "FROM covid19data " +
        "WHERE Confirmed > 0 " +
        "GROUP BY Country_Region " +
        "ORDER BY Confirmed ASC LIMIT 10")
    df.show()
    save("bottomConfirmed",df)
  }
  def bottomRecovered(): Unit = {
    /*---Bottom 10 recovered across countries---*/
    val df=spark.sql("SELECT DISTINCT " +
      "Country_Region AS Country,MAX(Recovered) AS Recovered " +
      "FROM covid19data " +
      "WHERE Recovered>0 " +
      "GROUP BY Country " +
      "ORDER BY Recovered ASC LIMIT 10").toDF()
    df.show()
    save("bottomRecovered",df)
  }
  def topDeaths(): Unit = {
    /*---Top 10 deaths across countries---*/
    spark.sql(
      "SELECT Country_Region AS Country, MAX(Deaths) AS Deaths " +
        "FROM covid19data " +
        "GROUP BY Country_Region " +
        "ORDER BY Deaths DESC LIMIT 10"
    ).createOrReplaceTempView("topDeaths")
    val df=spark.sql(
      "SELECT Country_Region AS Country, ObservationDate as Date, SUM(cd.Deaths) AS Deaths " +
        "FROM covid19data cd " +
        "JOIN topDeaths td ON td.Country=cd.Country_Region " +
        "GROUP BY cd.Country_Region,cd.ObservationDate " +
        "ORDER BY ObservationDate"
    ).toDF()
    spark.sql("SELECT * FROM topDeaths").show()
    save("topDeaths",df)
  }
  def bottomDeaths(): Unit = {
    /*---Bottom 10 deaths across countries---*/
    val df=spark.sql("SELECT DISTINCT " +
      "Country_Region AS Country,MAX(Deaths) AS Deaths " +
      "FROM covid19data " +
      "WHERE Deaths>0 " +
      "GROUP BY Country " +
      "ORDER BY Deaths ASC LIMIT 10").toDF()
    df.show()
    save("bottomDeaths",df)
  }
}

