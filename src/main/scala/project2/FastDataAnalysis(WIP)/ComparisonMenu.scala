package project2.`FastDataAnalysis(WIP)`


class ComparisonMenu {


  object ComparisonMenu {



    val compMenuWelcome: String = "Comparison Menu: Choose Two Countries and a predicate to compare them with."

    def compInit(): Unit = {

      val tb = new DataTableBuilder
      val q = new queries

      val choice1: String = "Cases"
      val choice2: String = "Deaths"
      val choice3: String = "Recovered"

      val comparison = new CountryVsCountry

      var count:Int = 0

      while(count < 1) {
        var inputUserMenu = scala.io.StdIn.readLine(s"Welcome to Comparison Menu. Here you can choose 2 countries and compare them based on $choice1 | $choice2 | $choice3. \nWould you like to continue? \n'Y' to continue or 'N' to return\nEntry:")
        if(inputUserMenu == "Y" || inputUserMenu == "y") {
        tb.dataTableCovid.Data_Table()
        q.selectAllCountryNames()
        val country1Input1: String = scala.io.StdIn.readLine("\nChoose first country from list \nEntry: ")
        val country2Input2: String = scala.io.StdIn.readLine("\nChoose second country from list \nEntry: ")
        val comparisonPredicate: String = scala.io.StdIn.readLine(s"\nChoose either $choice1,$choice2, or $choice3 \nEntry: ")


          comparisonPredicate match {
            case choice1 => {
              comparison.compare(country1Input1, country2Input2, choice1)

              var count2 = 0
              while(count2 < 1) {
                var inputReturn = scala.io.StdIn.readLine("Make another comparison? \n'Y' to continue or 'N' to return\nEntry:")
                if(inputReturn == "Y" || inputReturn == "y")
                  {
                    count2 = count2 + 1
                  }
                else if(inputReturn == "N" || inputReturn =="n")
                  {
                    count2 = count2 + 1
                    count = count + 1
                  }
                  else{println("Please Choose Either 'Y' or 'No'")}

              }
                }
            case choice2 => {
              comparison.compare(country1Input1, country2Input2, choice2)
              var count2 = 0
              while(count2 < 1) {
                var inputReturn = scala.io.StdIn.readLine("Make another comparison? \n'Y' to continue or 'N' to return\nEntry:")

                if(inputReturn == "Y" || inputReturn == "y")
                {
                  count2 = count2 + 1
                }
                else if(inputReturn == "N" || inputReturn =="n")
                {
                  count2 = count2 + 1
                  count = count + 1
                }
                else{println("Please Choose Either 'Y' or 'No'")}
              }
            }
            case choice3 => {
              comparison.compare(country1Input1, country2Input2, choice1)
              var count2 = 0
              while(count2 < 1) {
                var inputReturn = scala.io.StdIn.readLine("Make another comparison? \n'Y' to continue or 'N' to return\nEntry:")
                if(inputReturn == "Y" || inputReturn == "y")
                {
                  count2 = count2 + 1
                }
                else if(inputReturn == "N" || inputReturn =="n")
                {
                  count2 = count2 + 1
                  count = count + 1
                }
                else{println("Please Choose Either 'Y' or 'No'")}

              }
            }
          }
        }
        else if(inputUserMenu == "N" || inputUserMenu == "n")
          {
            count = count + 1
          }
        else
          {
            println("Please Enter 'Y' or 'N' ")
          }
      }






    }
  }
}