package project2

import org.apache.spark.sql.SparkSession

object Menus {
  /* this exists for my own testing purpose

  def main(args: Array[String]): Unit = {
    MainMenu("hi")
    // ALL 'util:string' will be replaced with 'util:SparkSession'
    // This is to demonstrate creating our own queries
  } */

  /*
  TODO:
    - Let's set up a date filter that can grab an earliest and latest date for a query
   */

  def MainMenu(util: SparkSession): Unit = {
    println("Welcome.")
    var quit = false
    do {
      println("[Data | Quit]")
      readLine.toLowerCase match {
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
        case "country" | "region" | "country/region" | "c/r" => FilterMenu(util, "country/region")
        case "state" | "province" | "state/province" | "s/p" => FilterMenu(util, "province/state")
        case "none" => SortMenu(util, "", "") //FilterMenu(util,"")
        case "back" => back = true
        case _ => println("Input unclear.")
      }
    } while (!back)
  }

  def FilterMenu(util: SparkSession, filter: String): Unit = {
    filter match {
      case "country/region" => {
        println("What country/region would you like to filter by?")
        SortMenu(util, filter, readLine)
      }
      case "province/state" => {
        println("What state/province would you like to filter by?")
        SortMenu(util, filter, readLine)
      }
      //case "" => SortMenu(util,filter,"")
    }
  }

  def SortMenu(util: SparkSession, filter: String, filterNote: String): Unit = {
    var where = ""
    if (filter != "") where = s" WHERE $filter=='$filterNote'"
    var sort = ""
    var acdc = ""

    println("\nWhat would you like to sort by?\nDefault is Date.")
    do {
      println("[Country/Region | State/Province | Date | Confirmed | Deaths | Recovered]")
      readLine.toLowerCase match {
        case "country" | "region" | "country/region" | "c/r" => sort = " ORDER BY country/region"
        case "state" | "province" | "state/province" | "s/p" => sort = " ORDER BY province/state"
        case "date" => sort = " ORDER BY date"
        case "confirmed" => sort = " ORDER BY confirmed"
        case "deaths" => sort = " ORDER BY deaths"
        case "recovered" => sort = " ORDER BY recovered"
        case _ => println("Input unclear.")
      }
    } while (sort == "")
    do {
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
    val query = s"SELECT * FROM covid19data$where$sort$asc"
    println(s"$query\n")
    util.sql(query).show(false)
  }
}
