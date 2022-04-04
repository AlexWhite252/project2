class queries {

  var s = new Sparker

  def selectAll(tableName:String):Unit=
  {

    s.spark().sql(s"Select * from $tableName")
  }


  def JoinTwoTables(Table1:String,Table2:String,Table1JoinColumn:String,Table2JoinColumn:String,Column1:String): Unit =
  {
    s.spark().sql(s"Select $Column1 from $Table1 INNER JOIN $Table2 on $Table1.$Table1JoinColumn == $Table2.$Table2JoinColumn")

  }

  def JoinTwoTables(Table1:String,Table2:String,Table1JoinColumn:String,Table2JoinColumn:String,Column1:String,Column2:String): Unit =
  {
    s.spark().sql(s"Select $Column1,$Column2 from $Table1 INNER JOIN $Table2 on $Table1.$Table1JoinColumn == $Table2.$Table2JoinColumn")

  }

  def JoinTwoTables(Table1:String,Table2:String,Table1JoinColumn:String,Table2JoinColumn:String,Column1:String,Column2:String,Column3:String): Unit =
  {
    s.spark().sql(s"Select $Column1,$Column2,$Column3 from $Table1 INNER JOIN $Table2 on $Table1.$Table1JoinColumn == $Table2.$Table2JoinColumn")

  }



}
