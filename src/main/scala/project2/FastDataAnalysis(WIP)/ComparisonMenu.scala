package project2.`FastDataAnalysis(WIP)`

import scala.collection.mutable.ListBuffer


class ComparisonMenu {


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
      val tb = new DataTableBuilder
      val q = new queries
      //Choices 1 2 and 3. Made so everywhere where you choose any of these it changes everywhere.
      val choice1: String = "Cases"
      val choice2: String = "Deaths"
      val choice3: String = "Recovered"
      tb.dataTableCovid.Data_Table()
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
                  println("Please Choose Either 'Y' or 'No'")
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


    def CountryListBuilder(): Unit = {
      var done: Boolean = false
      while (!done) {
        try {
          val test = scala.io.StdIn.readLine("Choose Another Country \n")
          var c = new CountryBuilder(test)
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