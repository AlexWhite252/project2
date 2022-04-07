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
    println("Welcome.")
    var quit = false
    do { // waiting until valid input
      println("[Data | Quit]")
      readLine.toLowerCase match { // go off of input
        case "data" => DataMenu(util)
        case "quit" => quit = true
        case _ => println("Input unclear.")
      }
    } while (!quit)
  }

  def DataMenu(util: SparkSession): Unit = {
    println("Filter?")
    var back = false
    do {
      println("[Country/Region | State/Province | None | Back]")
      readLine.toLowerCase match {
        case "country" | "region" | "country/region" | "c/r" => FilterMenu(util, "Country/Region") // set this to what the column is called in the table
        case "state" | "province" | "state/province" | "s/p" => FilterMenu(util, "Province/State") // set this to what the column is called in the table
        case "none" => SortMenu(util, "", "") //FilterMenu(util,"")
        case "back" => back = true
        case _ => println("Input unclear.")
      }
    } while (!back)
  }

  def FilterMenu(util: SparkSession, filter: String): Unit = {
    filter match {
      case "Country/Region" => {
        println("What country/region would you like to filter by?")
        SortMenu(util, filter, readLine) // Maybe set up a verifier or interpreter for this
      }
      case "Province/State" => {
        println("What state/province would you like to filter by?")
        SortMenu(util, filter, readLine) // verify readLine stuff
      }
      //case "" => SortMenu(util,filter,"")
    }
  }

  def SortMenu(util: SparkSession, filter: String, filterNote: String): Unit = {
    var where = ""
    if (filter != "") where = s" WHERE `$filter`=='$filterNote'" // If we have a filter, set the WHERE clause.
    var sort = ""
    var acdc = ""

    println("\nWhat would you like to sort by?\nDefault is Date.")
    do { // wait for a proper sort method.
      println("[Country/Region | State/Province | Date | Confirmed | Deaths | Recovered]")
      readLine.toLowerCase match {
        case "country" | "region" | "country/region" | "c/r" => sort = " ORDER BY 'Country/Region'"
        case "state" | "province" | "state/province" | "s/p" => sort = " ORDER BY 'Province/State'"
        case "date" => sort = " ORDER BY ObservationDate"
        case "confirmed" => sort = " ORDER BY Confirmed"
        case "deaths" => sort = " ORDER BY Deaths"
        case "recovered" => sort = " ORDER BY Recovered"
        case _ => println("Input unclear.")
      }
    } while (sort == "")
    do { // Up or down?
      println("Ascending or Descending?\n[ASC | DESC]")
      readLine.toLowerCase match {
        case "a" | "as" | "asc" => acdc = " ASC"
        case "d" | "de" | "des" | "desc" => acdc = " DESC"
        case _ => println("Input unclear.")
      }
    } while (acdc == "")
    GetQuery(util, where, sort, acdc)
  }

  def GetQuery(util: SparkSession, where: String, sort: String, asc: String): Unit = {
    val query = s"SELECT * FROM covid19data$where$sort$asc" // SELECT * FROM covid19data WHERE filter=filterSet ORDER BY column ASC
    println(s"$query\n") // this is for debug purposes
    util.sql(query).show(false)

    println("Export Query?")
    println("[as JSON | as CSV | No]") // It doesn't overwrite files yet. Will work on.
    readLine.toLowerCase match {
      case "json" | "as json" => {
        val tempy = util.sql(query) // take the query into an rdd/df
        val outpo = tempy.repartition(1) // repartition it so it makes only 1 file
        outpo.write.mode(SaveMode.Overwrite).json(System.getProperty("user.dir")+"\\export\\covid19data.json") // save the file
      }
      case "csv" | "as csv" => { // repeat from json, but as a csv
        val tempy = util.sql(query)
        val outpo = tempy.repartition(1)
        outpo.write.mode(SaveMode.Overwrite).csv(System.getProperty("user.dir")+"\\export\\covid19data.csv")
      }
      case _ => // nothing, we're leaving
    }
    /*
      val outpo = util.table(<query as a view?>) or util.sql(query)
      outpo.write.csv("yourstuff.csv") or outpo.write.json("yourstuff.json")
     */
  }

}
