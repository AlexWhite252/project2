import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Testmain {



  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    System.setProperty("hadoop.home.dir", "C:\\hadoop")
    val spark = SparkSession
      .builder
      .appName("IGDB query")
      .config("spark.master", "local[*]")
      .enableHiveSupport()
      .getOrCreate()
    println("Created Spark session\n")
    spark.sparkContext.setLogLevel("ERROR")


  //spark.sql("Drop table if exists covid19data")

    //spark.sql("CREATE TABLE covid19data (Sno Int, ObservationDate Date,Province_State String " +
    //  "NOT NULL, Country_Region String, Last_Update date,Confirmed Int, Deaths Int, Recovered Int)row format delimited " +
   //   "fields terminated by ',' stored as textfile").show(false)

   // spark.sql(s"LOAD DATA LOCAL INPATH 'covidTest_19_data.csv' " +
     // "INTO TABLE covid19data").show(100000,100000, false)

    //spark.sql("Select * from Covid_19_Data").show(1000000,false)0

    //spark.sql("SELECT Province_State,Country_Region,SUM(Deaths) AS Deaths FROM covid19data ORDER BY Deaths ASC LIMIT 10").show()

   val covidData=spark.read.format("csv").option("delimiter", ",").option("header", "true").option("inferSchema", "true").load("covidTest_19_data.csv")
  covidData.createOrReplaceTempView("covid19data")
   // spark.sql("SELECT Distinct Country_Region,SUM(Deaths) AS Deaths FROM covid19data Where Deaths > 0 and Country_Region Not Like '%\"' GROUP BY Country_Region ORDER BY Deaths ASC LIMIT 10").show()
   // spark.sql("SELECT DISTINCT Country_Region AS Country,SUM(Deaths) AS Deaths FROM covid19data WHERE Deaths>0 GROUP BY Country_Region ORDER BY Deaths ASC LIMIT 10").show()

    //spark.sql("Select Distinct Country_Region from covid19data").show(10000000,false)
    spark.sql("Describe Table covid19data").show()

     // var c = new CountryBuilder("Japan")
     // println(c.Country.ToString())


    //var c1 = new CountryBuilder("USA")


    //println(c1.Country.ToString())
    def compUI():Unit = {
      var Count: Int = 0
      println("~~~~~Compare UI Init~~~~~")
      spark.sql("Select Distinct Country_Region from covid19data").show(1000000, false)
      while (Count < 1) {
        val input1 = scala.io.StdIn.readLine("~~~Compare UI Init~~~\n Please pick 2 Countries from the List above\n Entry1: ")
        val input2 = scala.io.StdIn.readLine("\nEntry2:")
        val input3 = scala.io.StdIn.readLine("\n And what would you like to compare? \nEntry:")
        var countryfight = new CountryVsCountry(input1,input2).compare(input3)
        Count = Count +1
      }
    }

compUI()
      //spark.sql("SELECT SUM(Confirmed)as China_Conf from covid19data where Country_Region == 'Mainland China' union SELECT SUM(Confirmed)as not from covid19data where Country_Region != 'Mainland China'").show(1000000, false)
    //spark.sql("SELECT SUM(Confirmed)as not from covid19data where Country_Region != 'Mainland China'").show(1000000, false)
    //spark.sql("Select * from covid19data where Province_State ='Hubei'").show(100000,false)
    //var china :Int = 38368845



    /*---Code within object---*/

    /*---I put covid_19_data.csv into src/main/Data directory I created---*/

    /*---Changed Province/State to Province_State, Country/Region to Country_Region Last Update to Last_Update---*/


/**
      /*---Confirmed cases, deaths, recovered within FIRST 4 months---*/
      spark.sql("SELECT DISTINCT COUNT(Country_Region) AS Countries,SUM(Confirmed) AS Confirmed_Cases,SUM(Deaths) AS Deaths,SUM(Recovered) AS Recovered FROM covid19data WHERE ObservationDate BETWEEN '01/22/2020' AND '05/22/2020' AND Last_Update='2021-05-03 04:20:39'").show()

      /*---Confirmed cases, deaths, recovered within LAST 4 months---*/
      spark.sql("SELECT DISTINCT COUNT(Country_Region) AS Countries,SUM(Confirmed) AS Confirmed_Cases,SUM(Deaths) AS Deaths,SUM(Recovered) AS Recovered FROM covid19data WHERE ObservationDate BETWEEN '01/02/2021' AND '05/02/2021' AND Last_Update='2021-05-03 04:20:39'").show()

      /*---Confirmed in China vs rest of the world---*/
      spark.sql("SELECT SUM(Confirmed)as China_vs_World from covid19data where Country_Region == 'Mainland China' AND Last_Update='2021-05-03 04:20:39' union SELECT SUM(Confirmed)as not from covid19data where Country_Region != 'Mainland China' AND Last_Update='2021-05-03 04:20:39'").show(1000000, false)
      val countries = spark.sql("SELECT DISTINCT COUNT(Country_Region) FROM covid19data WHERE Country_Region!='%China%'").head().getLong(0)
      val confirmed = spark.sql("SELECT SUM(Confirmed) FROM covid19data WHERE Last_Update='2021-05-03 04:20:39'").head().getDouble(0)
      val chinaCases = 1-((countries/confirmed)*100)
      println(f"China had ${chinaCases}%.0f%% of worldwide confirmed cases")

      /*---Top 10 recovered across countries---*/
      spark.sql("SELECT DISTINCT Country_Region AS Country,SUM(Recovered) AS Recovered FROM covid19data WHERE Last_Update='2021-05-03 04:20:39' GROUP BY Country_Region ORDER BY Recovered DESC LIMIT 10").show()

      /*---Bottom 10 recovered across countries---*/
      spark.sql("SELECT DISTINCT Country_Region AS Country,SUM(Recovered) AS Recovered FROM covid19data WHERE Recovered>0 AND Last_Update='2021-05-03 04:20:39' GROUP BY Country_Region ORDER BY Recovered ASC LIMIT 10").show()

      /*---Top 10 deaths across countries---*/
      spark.sql("SELECT DISTINCT Country_Region AS Country,SUM(Deaths) AS Deaths FROM covid19data WHERE Last_Update='2021-05-03 04:20:39' GROUP BY Country_Region ORDER BY Deaths DESC LIMIT 10").show()

      /*---Bottom 10 deaths across countries---*/
      spark.sql("SELECT DISTINCT Country_Region AS Country,SUM(Deaths) AS Deaths FROM covid19data WHERE Deaths>0 AND Last_Update='2021-05-03 04:20:39' GROUP BY Country_Region ORDER BY Deaths ASC LIMIT 10").show()

**/

  }




}
