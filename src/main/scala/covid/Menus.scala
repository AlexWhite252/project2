package covid

import org.apache.spark.sql.functions.{col, to_date}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.io.StdIn.readLine

object Menus {
  // DEBUG MAIN
  /*
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\hadoop")
    val spark = SparkSession
      .builder
      .appName("COVID data query")
      .config("spark.master", "local[*]")
      .enableHiveSupport()
      .getOrCreate()
    println("Created Spark session\n")
    spark.sparkContext.setLogLevel("ERROR")

    var covid: DataFrame =spark.read.format("csv")
      .option("delimiter", ",")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("data/covid_19_data.csv")

    // need to convert the date entries into dates
    covid = covid.select(
      to_date(col("ObservationDate"),"MM/dd/yyyy").as("Date"),
      col("Province_State"),
      col("Country_Region"),
      col("Confirmed"),
      col("Deaths"),
      col("Recovered")
    )
    covid.createOrReplaceTempView("covid19data")

    MainMenu(spark)
  }
  // */

  /*
  TODO:
    - Will figure out over the weekend, please say if you see anything that needs improving.

  CURRENT:
    - All menus and filters functional
    - Sorting functional
    - Date filter functional
   */

  def MainMenu(util: SparkSession): Unit = {
    println("Welcome.")
    var quit = false
    do { // waiting until valid input
      println("[1. Data | 0. Quit]")
      readLine.toLowerCase match { // go off of input
        case "1" | "data" => DataMenu(util)
        case "0" | "quit" => quit = true
        case _ => println("Input unclear.")
      }
    } while (!quit)
  }

  def DataMenu(util: SparkSession): Unit = { // Decide what kind of filter you want.
    println("Filter?")
    var back = false
    do {
      println("[1. Country/Region | 2. State/Province | 5. None | 0. Back]")
      readLine.toLowerCase match {
        case "1" | "country" | "region" | "country/region" | "c/r" => FilterMenu(util, "Country_Region") // set this to what the column is called in the table
        case "2" | "state" | "province" | "state/province" | "s/p" => FilterMenu(util, "Province_State") // set this to what the column is called in the table
        case "5" | "none" => DateFilter(util, "", "") //FilterMenu(util,"")
        case "0" | "back" => back = true
        case _ => println("Input unclear.")
      }
    } while (!back)
  }

  def FilterMenu(util: SparkSession, filter: String): Unit = { // Decide on a value for your filter.
    filter match {
      case "Country_Region" => {
        println("What country/region would you like to filter by?")
        DateFilter(util, filter, readLine)
      }
      case "Province_State" => {
        println("What state/province would you like to filter by?")
        DateFilter(util, filter, readLine)
      }
      //case "" => SortMenu(util,filter,"")
    }
  }

  def DateFilter(util: SparkSession, filter:String, filterNote:String): Unit= { // If the user only wants data from a certain date range.
    println("Filter by date range?\n[Y/N]")
    var betw = ""

    do {
      readLine.toLowerCase match {
        case "y" => // get a date range
        {
          println("Enter a start date; YYYY-MM-DD")
          val st = readLine
          println("Enter an end date; YYYY-MM-DD")
          val en = readLine
          betw = s" Date BETWEEN '$st' AND '$en'"
        }
        case "n" => // do not
        {
          betw = "-" // break flag
        }
        case _ => println("Input unclear.")
      }
    } while (betw=="")
    if(betw=="-") betw="" // reset to empty

    SortMenu(util,filter,filterNote,betw)
  }

  def SortMenu(util: SparkSession, filter: String, filterNote: String, dateFilt: String): Unit = { // For sorting/ordering the list of data.
    var where = ""
    if (filter != "") where = s" WHERE `$filter`=='$filterNote'" // If we have a filter, set the WHERE clause.
    var sort = ""
    var acdc = ""

    println("\nWhat would you like to sort by?\nDefault is Date.")
    do { // wait for a proper sort method.
      println("[1. Country/Region | 2. State/Province | 3. Date | 4. Confirmed | 5. Deaths | 6. Recovered]")
      readLine.toLowerCase match {
        case "1" | "country" | "region" | "country/region" | "c/r" => sort = " ORDER BY 'Country_Region'"
        case "2" | "state" | "province" | "state/province" | "s/p" => sort = " ORDER BY 'Province_State'"
        case "3" | "date" => sort = " ORDER BY Date"
        case "4" | "confirmed" => sort = " ORDER BY Confirmed"
        case "5" | "deaths" => sort = " ORDER BY Deaths"
        case "6" | "recovered" => sort = " ORDER BY Recovered"
        case _ => println("Input unclear.")
      }
    } while (sort == "")
    do { // Up or down?
      println("Ascending or Descending?\n[1. ASC | 2. DESC]")
      readLine.toLowerCase match {
        case "1" | "a" | "as" | "asc" => acdc = " ASC"
        case "2" | "d" | "de" | "des" | "desc" => acdc = " DESC"
        case _ => println("Input unclear.")
      }
    } while (acdc == "")

    GetQuery(util,where,sort,acdc,dateFilt)
    //GetQuery(util, where, sort, acdc) (old parameters)
  }

  def GetQuery(util: SparkSession, where: String, sort: String, asc: String, between: String): Unit = { // Actually do query
    var query = ""
    if(where!="") {
      if (between!="") { // filter & date range
        query = s"SELECT * FROM covid19data$where AND$between$sort$asc"
      }else { // filter, no date range
        query = s"SELECT * FROM covid19data$where$sort$asc"
      }
    } else {
      if (between!="") { // no filter, date range
        query = s"SELECT * FROM covid19data WHERE$between$sort$asc"
      }else { // no filter or date range
        query = s"SELECT * FROM covid19data$sort$asc"
      }
    }
    //println(s"$query\n") // this is for debug purposes
    val wows = util.sql(query) // save it as a dataframe
    wows.show(false) // show it

    var coco = false
    do {
      println("Export Query?")
      println("[1. Yes | 0. No]")
      readLine.toLowerCase match {
        case "1" | "y" | "yes" => {
          val path = System.getProperty("user.dir")+"\\export\\covid19data" // grab working directory, and 'export' folder within.
          DFWriter.Write(path,wows) // good thing it's a dataframe.
          coco = true
        }
        case "0" | "n" | "no" => coco = true
        case _ => println("Input unclear.")
      }
    } while (!coco)
  }
}
