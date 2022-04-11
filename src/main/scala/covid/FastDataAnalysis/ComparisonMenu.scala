package covid.FastDataAnalysis



import scala.collection.mutable.ListBuffer


class ComparisonMenu {

  val tb = new DataTableBuilder
  tb.dataTableCovid.Data_Table()
  val q = new queries

  object ComparisonMenu {

    var country1Input = new String
    var country2Input = new String
    var countryList = new ListBuffer[String]
    val compMenuWelcome: String = "Comparison Menu: Choose Two Countries and a predicate to compare them with."

    def compHome(): Unit = {

      val homeInput = scala.io.StdIn.readLine("Welcome to Comparison Menu. \nOne on One  or one on Many?  Enter either 'One' or 'Many'\nEntry:")

      if (homeInput.equalsIgnoreCase("one")) {
        compInit()
      }
      else if (homeInput.equalsIgnoreCase("many")) {

      }


    }

    def compInit(): Unit = {
      //Creates a spark dataset

      //Choices 1 2 and 3. Made so everywhere where you choose any of these it changes everywhere.
      val choice1: String = "Cases"
      val choice2: String = "Deaths"
      val choice3: String = "Recovered"

      val comparison = new CountryVsCountry

      var count: Int = 0
      // This variable controls the infinite nature of the menu. Essentially you'll stay in this menu until Count == 1

      while (count < 1) {
        var inputUserMenu = scala.io.StdIn.readLine(s" Here you can choose 2 countries and compare them based on $choice1 | $choice2 | $choice3. \nWould you like to continue? \n'Y' to continue or 'N' to return\nEntry:")
        if (inputUserMenu == "Y" || inputUserMenu == "y") {

          q.selectAllCountryNames()

          println("\n--------\nCountry 1")
          Country1tester()
          println(s"Country 1: $country1Input")

          println("\n--------\nCountry 2")
          Country2tester()
          println(s"Country 2: $country2Input")

          val comparisonPredicate: String = scala.io.StdIn.readLine(s"\nChoose  |$choice1| |$choice2| |$choice3| \nEntry: ")


          comparisonPredicate match {
            case choice1 => {
              comparison.compare(country1Input, country2Input, choice1)
              var count2 = 0
              while (count2 < 1) {
                var inputReturn = scala.io.StdIn.readLine("Make another comparison? \n'Y' to continue or 'N' to return\nEntry:")
                if (inputReturn == "Y" || inputReturn == "y") {
                  count2 = count2 + 1
                }
                else if (inputReturn == "N" || inputReturn == "n") {
                  count2 = count2 + 1
                  count = count + 1
                }
                else {
                  println("Please Choose Either 'Y' or 'No'")
                }

              }
            }
            case choice2 => {
              comparison.compare(country1Input, country2Input, choice2)
              var count2 = 0
              while (count2 < 1) {
                var inputReturn = scala.io.StdIn.readLine("Make another comparison? \n'Y' to continue or 'N' to return\nEntry:")

                if (inputReturn == "Y" || inputReturn == "y") {
                  count2 = count2 + 1
                }
                else if (inputReturn == "N" || inputReturn == "n") {
                  count2 = count2 + 1
                  count = count + 1
                }
                else {
                  println("Please Choose Either 'Y' or 'No'")
                }
              }
            }
            case choice3 => {
              comparison.compare(country1Input, country2Input, choice1)
              var count2 = 0
              while (count2 < 1) {
                var inputReturn = scala.io.StdIn.readLine("Make another comparison? \n'Y' to continue or 'N' to return\nEntry:")
                if (inputReturn == "Y" || inputReturn == "y") {
                  count2 = count2 + 1
                }
                else if (inputReturn == "N" || inputReturn == "n") {
                  count2 = count2 + 1
                  count = count + 1
                }
                else {
                  println("Please Choose Either 'Y' or 'N'")
                }

              }
            }
          }
        }
        else if (inputUserMenu == "N" || inputUserMenu == "n") {
          count = count + 1
        }
        else {
          println("Please Enter 'Y' or 'N' ")
        }
      }


      def mulicompInit(): Unit = {

        println("\n--------\nCountry 1")
        Country1tester()

        var countListCount = 0


        while (countListCount < 1) {


        }


        val choice1: String = "Cases"
        val choice2: String = "Deaths"
        val choice3: String = "Recovered"
        val tb = new DataTableBuilder
        val q = new queries


      }


    }

    def Country1tester(): Unit = {
      var countryInput = new String
      try {
        val test = scala.io.StdIn.readLine("\nChoose a country from list \nEntry: ")

        val c1Test = new CountryBuilder(test)
        c1Test.Country

        country1Input = test

      }
      catch {
        case wrongCountryName: NullPointerException => {
          println("Country Not Found")
          countryInput = new String
          Country1tester()
        }
        //case wrongCountryStart: NullPointerException =>
      }
      //println(s"This Is the country Name $countryInput")
    }

    def Country2tester(): Unit = {
      try {
        val test = scala.io.StdIn.readLine("\nChoose a country from list \nEntry: ")
        val c1Test = new CountryBuilder(test)
        c1Test.Country
        country2Input = test

      }
      catch {
        case wrongCountryName: NullPointerException => {
          println("Country Not Found")

          Country2tester()
        }
        //case wrongCountryStart: NullPointerException =>
      }
      //println(s"This Is the country Name $country2Input")
    }

    def CountryListInit(): Unit ={
      var count = 0
      var userInputSolo = new String
      var userInputList = new String
      var Listnum = 0

      println("In this Menu you can choose 1 country and find the % difference between a group of countries")
      while(count <1)
        {
          val manyMenuAuthen = scala.io.StdIn.readLine("Would you like to contine? \nWould you like to continue? \n'Y' to continue or 'N' to return\nEntry:")
          if(manyMenuAuthen.equalsIgnoreCase("Y"))
            {

              userInputSolo = scala.io.StdIn.readLine("Please enter first country for comparison\nEntry:")
              CountryListTester(userInputSolo)


            }
          else if(manyMenuAuthen.equalsIgnoreCase("N"))
            {
              println("Okay! Returning")
              count = count + 1
            }
          else
            {
              println("Please Choose Either 'Y' or 'N'")
            }





        }








    }

    def CountryListBuilder(): Unit = {
      var countries = new ListBuffer[String]
      var userInput = new String
      var count = 0
      var testInput = new String
      while(count < 1)
        {
          userInput = scala.io.StdIn.readLine("Would you like to add a Country to comparison group?\n'Y' to continue or 'N' to return\nEntry:")
          if(userInput.equalsIgnoreCase("Y"))
            {
              testInput = scala.io.StdIn.readLine("Please add a country from the list to test.")
              CountryListTester(testInput)
            }
          else if(userInput.equalsIgnoreCase("N"))
            {
              count = count + 1
            }
          else
            {
              println("Please Choose Either 'Y' or 'N'")
            }
        }










    }

    def CountryListTester(cName:String): Unit = {
      var done: Boolean = false
      while (!done) {
        try {
          var c = new CountryBuilder(cName)
          done = true
        }
        catch {
          case wrongCountryName: NullPointerException => {
            println("Country Not Found")
          }
        }
       // countryList += countriesInput
      }
    }
  }
}