package covid.FastDataAnalysis

import org.apache.spark.sql.SparkSession


class CountryBuilder(name:String,spark: SparkSession) {

  
  val countryName: String = name

  object Country {


      var Longi: Double = 0
      var Latt: Double = 0
      //var MaxDate: String = spark.sql(s"Select MAX(Last_Update) from covid19data WHERE Country_Region = '$countryName'").head().getTimestamp(0).toString



   // spark.sql(s"Select MAX(Last_Update) from covid19data WHERE Country_Region = '$countryName'").show(10000, false)

      //println(s"This is the Country: $countryName")
      //println(s"This is the max Date: $MaxDate")
     // println(s"This is the name:$countryName")

     // spark.spark.sql(s"Select Confirmed from covid19data where Country_Region ='$countryName' and Last_Update = '$MaxDate' ").show(1000000,false)
      val Total_Cases:Int= spark.sql(s"Select MAX(Confirmed) from covid19data where Country_Region ='$countryName'").head().getInt(0)
      val Total_Deaths:Int = spark.sql(s"SELECT MAX(Deaths) FROM covid19data WHERE Country_Region = '$countryName'").head().getInt(0)
      val Total_Recovered:Int = spark.sql(s"SELECT MAX(Recovered) FROM covid19data WHERE Country_Region = '$countryName'").head().getInt(0)

      //Longi = spark.spark.sql(s"Select Long from Covid_Deaths where country ='$name' ").head().getDouble(3)
      //Longi = spark.spark.sql(s"Select Latt from Covid_Deaths where country ='$name' ").head().getDouble(3)

    def getDeaths:Int= Total_Deaths
    def getCases:Int = Total_Cases
    def getRecovered:Int= Total_Recovered
    def getName:String= countryName


     def ToString:String={
      var output = new String
       output = s"The Country_Region of $countryName. || Confirmed Covid Cases:$Total_Cases || Total Deaths:$Total_Deaths || Total Recovered:$Total_Recovered"
      output
      }


  }

}
