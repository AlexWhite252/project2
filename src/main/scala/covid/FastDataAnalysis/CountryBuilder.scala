package covid.FastDataAnalysis
class CountryBuilder(name:String) {


  var spark = new Sparker
  val countryName = name

  object Country {


      var Longi: Double = 0
      var Latt: Double = 0
      var MaxDate = spark.spark().sql(s"Select MAX(Last_Update) from covid19data WHERE Country_Region = '$countryName'").head().getString(0)



    spark.spark().sql(s"Select MAX(Last_Update) from covid19data WHERE Country_Region = '$countryName'").show(10000, false)

      println(s"This is the Country: $countryName")
      println(s"This is the max Date: $MaxDate")
     // println(s"This is the name:$countryName")

     // spark.spark.sql(s"Select Confirmed from covid19data where Country_Region ='$countryName' and Last_Update = '$MaxDate' ").show(1000000,false)
      val Total_Cases:Int= spark.spark.sql(s"Select Confirmed from covid19data where Country_Region ='$countryName' and Last_Update = '$MaxDate'").head().getInt(0)
      val Total_Deaths:Int = spark.spark.sql(s"SELECT Deaths FROM covid19data WHERE Country_Region = '$countryName' and Last_Update = '$MaxDate'").head().getInt(0)
      val Total_Recovered:Int = spark.spark.sql(s"SELECT Recovered FROM covid19data WHERE Country_Region = '$countryName' and Last_Update = '$MaxDate'").head().getInt(0)

      //Longi = spark.spark.sql(s"Select Long from Covid_Deaths where country ='$name' ").head().getDouble(3)
      //Longi = spark.spark.sql(s"Select Latt from Covid_Deaths where country ='$name' ").head().getDouble(3)

    def getDeaths():Int= {return Total_Deaths}
    def getCases():Int = {return Total_Cases}
    def getRecovered():Int={return Total_Recovered}
    def getName():String= {return countryName}


     def ToString():String={

      var output = new String
       output = s"The Country_Region of $countryName. || Confirmed Covid Cases:$Total_Cases || Total Deaths:$Total_Deaths || Total Recovered:$Total_Recovered || as of $MaxDate "
      return output
      }


  }

}
