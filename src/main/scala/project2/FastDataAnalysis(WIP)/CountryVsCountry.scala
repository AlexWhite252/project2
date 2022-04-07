package project2.`FastDataAnalysis(WIP)`



class CountryVsCountry() {


    def compare(Country1:String,Country2:String,CompareOn:String):Unit = {
      var Count:Int = 0
      var output = new String
      val c1 = new CountryBuilder(Country1)
      val c2 = new CountryBuilder(Country2)
      val c1Name = c1.Country.getName()
      val c2Name = c2.Country.getName()
      var c1CompNum:BigDecimal = 0
      var c2CompNum:BigDecimal = 0
      var comp:BigDecimal = 0
        while(Count < 1) {
          if (CompareOn == "Deaths") {

            c1CompNum = c1.Country.getDeaths()
            c2CompNum = c2.Country.getDeaths()

              comp = (c1CompNum / c2CompNum) * 100

            if(c1CompNum > c2CompNum)
              {
              output = s"When comparing $CompareOn: $c1Name has $comp% more $CompareOn than $c2Name"
              }
            else if(c1CompNum<c2CompNum)
              {
              output = s"When comparing $CompareOn: $c1Name has $comp% less $CompareOn than $c2Name"
              }
            else if(c1CompNum == c2CompNum)
              {
                output = s"When comparing $CompareOn: $c1Name is equal to $c2Name"
              }
            else
              {
                println("OOOOOOF Wrong you're math sucks")
              }
            //println(s"The Comp: $comp")
            //output = s"When comparing $CompareOn: $c1Name has $comp% against $c2Name"
            Count = Count + 1
            //println(s"$c1Name: Number of Deaths ${c1.Country.getDeaths()}  \n $c2Name: Number of Deaths ${c2.Country.getDeaths()}")
          }
          else if (CompareOn == "Recovered") {

            c1CompNum = c1.Country.getRecovered()
            c2CompNum = c2.Country.getRecovered()
            comp = (c1CompNum / c2CompNum) * 100

            if(c1CompNum > c2CompNum)
            {
              output = s"When comparing $CompareOn: $c1Name has $comp% more $CompareOn than $c2Name"
            }
            else if(c1CompNum<c2CompNum)
            {
              output = s"When comparing $CompareOn: $c1Name has $comp% less $CompareOn than $c2Name"
            }
            else if(c1CompNum == c2CompNum)
            {
              output = s"When comparing $CompareOn: $c1Name is equal to $c2Name"
            }
            else
            {
              println("OOOOOOF, Wrong, your math sucks")
            }

            //output = s"When comparing $CompareOn: $c1Name has $comp% against $c2Name"
            Count = Count + 1
            println(s"$c1Name: Number of Recovered ${c1.Country.getRecovered()}  \n $c2Name: Number of Recovered ${c2.Country.getRecovered()}")
          }
          else if (CompareOn == "Cases") {
            c1CompNum = c1.Country.getCases()
            c2CompNum = c2.Country.getCases()
            comp = (c1CompNum / c2CompNum) * 100


            if(c1CompNum > c2CompNum)
            {
              output = s"When comparing $CompareOn: $c1Name has $comp% more $CompareOn than $c2Name"
            }
            else if(c1CompNum<c2CompNum)
            {
              output = s"When comparing $CompareOn: $c1Name has $comp% less $CompareOn than $c2Name"
            }
            else if(c1CompNum == c2CompNum)
            {
              output = s"When comparing $CompareOn: $c1Name is equal to $c2Name"
            }
            else
            {
              println("OOOOOOF Wrong you're math sucks")
            }

            //output = s"When comparing $CompareOn: $c1Name has $comp% against $c2Name"
            Count = Count + 1
           println(s"$c1Name: Number of Cases ${c1.Country.getCases()}  \n$c2Name: Number of Cases ${c2.Country.getCases()}")
          }
          else {
            println("Error. Compare either Deaths, Recovered, or Cases")
          }
        }

        println(output)


      }
  def compare(Country1:String,Countries:List[String],CompareOn:String):Unit =
    {
      val c1 = Country1
      val countries = Countries
      val countryIndex:Int = countries.length
      var countriesName= new String
      //countries.foreach(countryNames => String)
    }


  }





