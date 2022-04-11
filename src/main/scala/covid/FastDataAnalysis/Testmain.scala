package covid.FastDataAnalysis



import org.apache.log4j.{Level, Logger}

object Testmain {


  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    var c = new ComparisonMenu
    //c.ComparisonMenu.CompInit()
    //c.ComparisonMenu.CompHome()
    c.ComparisonMenu.CountryListInit()


  }

}



