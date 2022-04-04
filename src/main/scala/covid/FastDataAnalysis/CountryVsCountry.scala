package covid.FastDataAnalysis
class CountryVsCountry() {


    def compare(Country1:String,Country2:String,CompareOn:String):Unit = {
        var Count:Int = 0
        var output = new String
      var c1 = new CountryBuilder(Country1)
      var c2 = new CountryBuilder(Country2)
      var c1Name = c1.Country.getName()
      var c2Name = c2.Country.getName()
        while(Count < 1) {
          if (CompareOn == "Deaths") {
              val comp:Double=(c1.Country.getDeaths() /c2.Country.getDeaths() )*100
              output = s"When comparing $CompareOn: $c1Name has $comp% against $c2Name"
            Count = Count + 1
          }
          else if (CompareOn == "Recovered") {

            val comp:Double =(c1.Country.getRecovered() / c2.Country.getRecovered())*100
            output = s"When comparing $CompareOn: $c1Name has $comp% against $c2Name"
            Count = Count + 1
          }
          else if (CompareOn == "Confirmed") {
            val comp:Double=(c1.Country.getCases()/c2.Country.getCases())*100
            output = s"When comparing $CompareOn: $c1Name has $comp% against $c2Name"
            Count = Count + 1
          }
          else {
            println("Error. Compare either Deaths, Recovered, or Confirmed")
          }
        }

        println(output)


      }
  def compare(Country1:String,Countries:List[String],CompareOn:String ):Unit =
    {

      val c1 = Country1
      val countries = Countries

      val countryIndex:Int = countries.length
      var countriesName= new String


      countries.foreach(countryNames => String)








    }


  }





