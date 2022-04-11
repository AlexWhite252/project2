package covid

import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.io.StdIn.readLine

object Menus {
  /* this exists for my own testing purpose

  def main(args: Array[String]): Unit = {
    MainMenu("hi")
    // ALL 'util:string' will be replaced with 'util:SparkSession'
    // This is to demonstrate creating our own queries
  } */

  /*
  TODO:
    - Date filter (Earliest/Latest dates)

  CURRENT:
    - All menus and filters functional
    - Sorting functional
    - Exporting functional (although messy, because spark messy??)
   */

  def MainMenu(util: SparkSession): Unit = {
    val sq = new SparkQueries(util)
    println("Welcome")
    var quit = false
    do { // waiting until valid input
      println("Please select an option:" +
        "\n [1] Cases from January 2020 to April 2020" +
        "\n [2] Cases from February 2021 to May 2021" +
        "\n [3] Cases in China compared to the rest of the world" +
        "\n [4] Timeline of the top ten countries with the highest recovery rate" +
        "\n [5] Timeline of the top ten countries with the highest mortality rate" +
        "\n [6] Timeline of the top ten countries with the highest number of confirmed cases" +
        "\n [7] Top ten countries with the lowest recovery rate" +
        "\n [8] Top ten countries with the lowest mortality rate" +
        "\n [9] Top ten countries with the lowest number of confirmed cases" +
        "\n[10] Custom Query" +
        "\n[Quit]\n")
      readLine.toLowerCase match { // go off of input
        case "1"=> sq.confirmedFirst()
        case "2"=> sq.confirmedLast()
        case "3"=> sq.ChinaVsTheWorld()
        case "4"=> sq.topRecovered()
        case "5"=> sq.topDeaths()
        case "6"=> sq.topConfirmed()
        case "7"=> sq.bottomRecovered()
        case "8"=> sq.bottomDeaths()
        case "9"=> sq.bottomConfirmed()
        case "10" => DataMenu(util)
        case "quit" => quit = true
        case _ => println("Input unclear")
      }
    } while (!quit)
  }

  def DataMenu(util: SparkSession): Unit = {
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

  def FilterMenu(util: SparkSession, filter: String): Unit = { // Really not a filter menu, just an input frame.
    filter match {
      case "Country_Region" => {
        println("What country/region would you like to filter by?")
        DateFilter(util, filter, readLine) // Maybe set up a verifier or interpreter for this
      }
      case "Province_State" => {
        println("What state/province would you like to filter by?")
        DateFilter(util, filter, readLine) // verify readLine stuff
      }
      //case "" => SortMenu(util,filter,"")
    }
  }

  def DateFilter(util: SparkSession, filter:String, filterNote:String): Unit= {
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
          betw = s" ObservationDate BETWEEN '$st' AND '$en'"
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

  def SortMenu(util: SparkSession, filter: String, filterNote: String, dateFilt: String): Unit = {
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
        case "3" | "date" => sort = " ORDER BY ObservationDate"
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
    //GetQuery(util, where, sort, acdc)
  }

  def GetQuery(util: SparkSession, where: String, sort: String, asc: String, between: String): Unit = {
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
    val wows = util.sql(query)
    wows.show(false)
    println("Export Query?")
    println("[as JSON | as CSV | No]") // It doesn't overwrite files yet. Will work on.
    readLine.toLowerCase match {
      case "json" | "as json" => {
        val df = util.sql(query) // take the query into an rdd/df
        DFWriter.JSON("data/customQuery",df)//.json
      }
      case "csv" | "as csv" => { // repeat from json, but as a csv
        val df = util.sql(query)
        DFWriter.CSV("data/customQuery",df)//.csv
      }
      case _ => // nothing, we're leaving
    }
    /*
      val outpo = util.table(<query as a view?>) or util.sql(query)
      outpo.write.csv("yourstuff.csv") or outpo.write.json("yourstuff.json")
     */
  }

}
