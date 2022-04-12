package covid.FastDataAnalysis



import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ListBuffer


class ComparisonMenu(spark: SparkSession) {

  //val tb = new DataTableBuilder
  //tb.dataTableCovid.Data_Table()
  val q = new queries(spark)
  val choice1: String = "Cases"
  val choice2: String = "Deaths"
  val choice3: String = "Recovered"
  val comparison = new CountryVsCountry(spark)

  object ComparisonMenu {

    var country1Input = new String
    var country2Input = new String
    //var countryList = new ListBuffer[String]
    val compMenuWelcome: String = "Comparison Menu: Choose Two Countries and a predicate to compare them with."
    var countryObjectList = new ListBuffer[CountryBuilder]

    def CompHome(): Unit = {

      val homeInput = scala.io.StdIn.readLine("Welcome to Comparison Menu\nOne to One or One to Many?\n[One | Many]\n")

      if (homeInput.equalsIgnoreCase("one")) {
        CompInit()
      }
      else if (homeInput.equalsIgnoreCase("many")) {

        CountryListInit()
      }


    }

    def CompInit(): Unit = {
      //Creates a spark dataset

      //Choices 1 2 and 3. Made so everywhere where you choose any of these it changes everywhere.


      var count: Int = 0
      // This variable controls the infinite nature of the menu. Essentially you'll stay in this menu until Count == 1

      while (count < 1) {
        var inputUserMenu = scala.io.StdIn.readLine(s"Here you can choose 2 countries and compare them based on $choice1 | $choice2 | $choice3. \nWould you like to continue? \n[Yes | No]\n")
        if (inputUserMenu.equalsIgnoreCase("yes")) {

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
                var inputReturn = scala.io.StdIn.readLine("Make another comparison? \n[Yes | No]\n")
                if (inputReturn.equalsIgnoreCase("yes")) {
                  count2 = count2 + 1
                }
                else if (inputReturn.equalsIgnoreCase("no")) {
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
                var inputReturn = scala.io.StdIn.readLine("Make another comparison? \n[Yes | No]\n")

                if (inputReturn.equalsIgnoreCase("yes")) {
                  count2 = count2 + 1
                }
                else if (inputReturn.equalsIgnoreCase("no")) {
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
                var inputReturn = scala.io.StdIn.readLine("Make another comparison? \n[Yes | No]\n")
                if (inputReturn.equalsIgnoreCase("yes")) {
                  count2 = count2 + 1
                }
                else if (inputReturn.equalsIgnoreCase("no")) {
                  count2 = count2 + 1
                  count = count + 1
                }
                else {
                  println("Please Choose Either 'Yes' or 'No'")
                }

              }
            }
            case _=>
          }
        }
        else if (inputUserMenu.equalsIgnoreCase("no")) {
          count = count + 1
        }
        else {
          println("Please Enter 'Yes' or 'No' ")
        }
      }


      def mulicompInit(): Unit = {

        println("\n--------\nCountry 1")
        Country1tester()

        var countListCount = 0


        while (countListCount < 1) {


        }


      }


    }

    def Country1tester(): Unit = {
      var countryInput = new String
      try {
        val test = scala.io.StdIn.readLine("\nChoose a country from list\n")

        val c1Test = new CountryBuilder(test,spark)


        country1Input = test
        println(s"${c1Test.Country.ToString}")
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
        val test = scala.io.StdIn.readLine("\nChoose a country from list\n")
        val c1Test = new CountryBuilder(test,spark)
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

    //Initializes the portion of the comparison submenu that allows user to compare 1 to many
    def CountryListInit(): Unit = {
      var count = 0
      var userInputSolo = new String
      var userInputList = new String
      var Listnum = 0

      println("In this Menu you can choose 1 country and find the % difference between a group of countries")
      while (count < 1) {
        val manyMenuAuthen = scala.io.StdIn.readLine("Would you like to continue? \n[Yes | No]\n")
        if (manyMenuAuthen.equalsIgnoreCase("Yes")) {
          q.selectAllCountryNames()
          Country1tester()

          CountryListBuilder()
          CountryListComparison()
        }
        else if (manyMenuAuthen.equalsIgnoreCase("No")) {
          println("Okay! Returning")
          count = count + 1
        }
        else {
          println("Please Choose Either 'Yes' or 'No'")
        }
      }
    }

    //creates the list that will be used in the comparison. Checks to make sure each entered country exists within the database
    def CountryListBuilder(): Unit = {

      var userInput = new String
      var count = 0
      //var testInput = new String



      while (count < 1) {
        var userCount = 0

        while (userCount < 1) {
          userInput = scala.io.StdIn.readLine("\n\nWould you like to add a Country to the comparison group?\n[Yes | No]\n")
          if (userInput.equalsIgnoreCase("Yes")) {

            var count2 = 0
            while (count2 < 1) {
              val userinput = scala.io.StdIn.readLine("\nEnter Country\n")
              var testingCountry: Boolean = false
              testingCountry=CountryListTester(userinput)



              if (testingCountry) {

                val c = new CountryBuilder(userinput,spark)
                countryObjectList += c

                for (country <- countryObjectList) {
                  println(country.Country.ToString)
                }
                count2 = count2 + 1
              }
              else {
                println("Please enter valid country")
              }
            }

          }
          else if (userInput.equalsIgnoreCase("No")) {
            count = count + 1
            userCount = userCount + 1
          }
          else {
            println("Please Choose Either 'Yes' or 'No'")
          }
        }

      }
    }

    //Tests that each country entered within the list
    def CountryListTester(cName: String): Boolean = {

      var exist: Boolean = false
      println(s"Testing: $cName against database")

      try {
        //spark.sql(s"Select * from covid19data where Country_Region == '$cName'").show(false)
        val Total_Cases:Int= spark.sql(s"Select MAX(Confirmed) from covid19data where Country_Region ='$cName'").head().getInt(0)
        exist = true

        //var c = new CountryBuilder(cName,spark)
      }
      catch {

        case wrongCountryName: NullPointerException => {
          println("Country Not Found")
          exist = false
          //println(exist)
        }
          exist = false
      }
     // println(exist)
      exist
    }

    def CountryListComparison(): Unit = {
      val comparisonPredicate = scala.io.StdIn.readLine(s"\nChoose  |$choice1| |$choice2| |$choice3| \n")
      val compmulti = new CountryVsCountry(spark)
      comparison.compare(country1Input, countryObjectList, comparisonPredicate)
    }

  }


  }
