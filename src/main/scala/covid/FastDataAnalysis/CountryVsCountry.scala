package covid.FastDataAnalysis


import org.apache.spark.sql.SparkSession

import java.math.{MathContext, RoundingMode}
import scala.collection.mutable.ListBuffer


class CountryVsCountry(spark: SparkSession) {



  def compare(Country1:String,Country2:String,CompareOn:String):Unit = {
      var Count:Int = 0
      var output = new String
      val c1 = new CountryBuilder(Country1,spark)
      val c2 = new CountryBuilder(Country2,spark)
      val c1Name = c1.Country.getName
      val c2Name = c2.Country.getName
      var c1CompNum:BigDecimal = 0
      var c2CompNum:BigDecimal = 0
      var comp:BigDecimal = 0

      var m = new MathContext(4,RoundingMode.HALF_UP)


        while(Count < 1) {
          if (CompareOn == "Deaths") {

            c1CompNum = c1.Country.getDeaths
            c2CompNum = c2.Country.getDeaths
          try {
            comp = (c1CompNum / c2CompNum) * 100
          }
            catch
            {
              case zero:ArithmeticException=> println("Divided By Zero")
            }
            if(c1CompNum > c2CompNum)
              {
              output = s"When comparing Total $CompareOn: $c1Name has ${comp.round(m)}% more $CompareOn than $c2Name"
              }
            else if(c1CompNum<c2CompNum)
              {
              output = s"When comparing Total $CompareOn: $c1Name has ${comp.round(m)}% less $CompareOn than $c2Name"
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

            c1CompNum = c1.Country.getRecovered
            c2CompNum = c2.Country.getRecovered
            try {
              comp = ((c1CompNum.abs-c2CompNum.abs) / ((c1CompNum +c2CompNum)/2) )* 100
            }
            catch
              {
                case zero:ArithmeticException=> println("Divided By Zero")
              }

            if(c1CompNum > c2CompNum)
            {
              output = s"When comparing Total $CompareOn, $c1Name has ${comp.round(m)}% more $CompareOn than $c2Name"
            }
            else if(c1CompNum<c2CompNum)
            {
              output = s"When comparing Total $CompareOn, $c1Name has ${comp.round(m)}% less $CompareOn than $c2Name"
            }
            else if(c1CompNum == c2CompNum)
            {
              output = s"When comparing Total $CompareOn, $c1Name is equal to $c2Name"
            }
            else
            {
              println("OOOOOOF, Wrong, your math sucks")
            }

            //output = s"When comparing $CompareOn: $c1Name has $comp% against $c2Name"
            Count = Count + 1
            println(s"$c1Name: Number of Recovered ${c1.Country.getRecovered}  \n$c2Name: Number of Recovered ${c2.Country.getRecovered}")
          }
          else if (CompareOn == "Cases") {
            c1CompNum = c1.Country.getCases
            c2CompNum = c2.Country.getCases
            try {
              comp = ((c1CompNum.abs-c2CompNum.abs) / ((c1CompNum +c2CompNum)/2) )* 100
            }
            catch
            {
              case zero:ArithmeticException=> println("Divided By Zero")
            }

            if(c1CompNum > c2CompNum)
            {
              output = s"When comparing Total $CompareOn, $c1Name has ${comp.round(m)}% more $CompareOn than $c2Name"
            }
            else if(c1CompNum<c2CompNum)
            {
              output = s"When comparing $CompareOn, $c1Name has ${comp.round(m)}% less $CompareOn than $c2Name"
            }
            else if(c1CompNum == c2CompNum)
            {
              output = s"When comparing Total $CompareOn, $c1Name is equal to $c2Name"
            }
            else
            {
              println("OOOOOOF Wrong you're math sucks")
            }

            //output = s"When comparing $CompareOn: $c1Name has $comp% against $c2Name"
            Count = Count + 1
           println(s"$c1Name: Number of Cases ${c1.Country.getCases}  \n$c2Name: Number of Cases ${c2.Country.getCases}")
          }
          else {
            println("Error. Compare either Deaths, Recovered, or Cases")
          }
        }

        println(output)


      }


  def compare(Country1:String,countryObjectList:ListBuffer[CountryBuilder],comparisonPredicate:String):Unit =
    {
      val c1 = new CountryBuilder(Country1,spark)
      var c1name = (s"|$Country1|")
      var subtotal:BigDecimal = 0
      var finalTotal:BigDecimal = 0
      var country1Total:BigDecimal = 0

      var m = new MathContext(4,RoundingMode.HALF_UP)

      var countriesNames:String = ""

      println(s"Country 1: ${c1.Country.getName} \n List of countries:")

      for (country <- countryObjectList) {
        println(country.Country.ToString)
      }

      if(comparisonPredicate.equalsIgnoreCase("Cases")) {
        var counterCountryList = countryObjectList.length
        while(counterCountryList != 0)
        {
          subtotal= subtotal + countryObjectList(counterCountryList  -1).Country.getCases
          countriesNames = countriesNames + "|" + countryObjectList(counterCountryList - 1).Country.getName
          counterCountryList = counterCountryList -1
          println(s"This is the subtotal: $subtotal")

        }
        country1Total =c1.Country.getCases

        try {
          finalTotal = (((country1Total).abs - (subtotal.abs)) / ((country1Total +subtotal)/2) * 100)
        }
        catch
        {
          case zero:ArithmeticException=> println("Divided By Zero")
        }
      }
      else if(comparisonPredicate.equalsIgnoreCase("Deaths")) {

        var counterCountryList = countryObjectList.length
        while(counterCountryList != 0)
        {
          subtotal= subtotal + countryObjectList(counterCountryList  -1).Country.getDeaths
          countriesNames = countriesNames + "|" + countryObjectList(counterCountryList - 1).Country.getName
          counterCountryList = counterCountryList -1
          println(s"This is the subtotal: $subtotal")

        }
        country1Total =c1.Country.getDeaths

        try {
          finalTotal = (((country1Total).abs - (subtotal.abs)) / ((country1Total +subtotal)/2) * 100)
        }
        catch
        {
          case zero:ArithmeticException=> println("Divided By Zero")
        }

        }
      else if(comparisonPredicate.equalsIgnoreCase("recovered")) {

        var counterCountryList = countryObjectList.length
        while(counterCountryList != 0)
        {
          subtotal= subtotal + countryObjectList(counterCountryList  -1).Country.getRecovered
          countriesNames = countriesNames + "|" + countryObjectList(counterCountryList - 1).Country.getName
          counterCountryList = counterCountryList -1
          println(s"This is the subtotal: $subtotal")
        }
        country1Total =c1.Country.getRecovered

        try {
          finalTotal = (((country1Total).abs - (subtotal.abs)) / ((country1Total +subtotal)/2) * 100)
        }
        catch
        {
          case zero:ArithmeticException=> println("Divided By Zero")
        }


      }
      else
      {

      }

      countriesNames = countriesNames + "|"
      if(country1Total< subtotal)
      {
        println(s"When comparing total $comparisonPredicate of $c1name to  the total $comparisonPredicate of $countriesNames,  $c1name had ${finalTotal.round(m)}% less $comparisonPredicate than $countriesNames")
      }
      else if(country1Total > subtotal)
      {
        println(s"When comparing total $comparisonPredicate of $c1name to the total $comparisonPredicate of $countriesNames,  $c1name had ${finalTotal.round(m)}% more $comparisonPredicate than $countriesNames")
      }
      else
      {
        println(s"When comparing total $comparisonPredicate of $c1name to the total $comparisonPredicate of $countriesNames,  $c1name had equal the amount of $comparisonPredicate compared $countriesNames")
      }
        }




      //countries.foreach(countryNames => String)
    }








