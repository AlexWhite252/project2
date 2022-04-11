name := "Project 2"

version := "0.2"

scalaVersion := "2.11.12"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.0"
libraryDependencies += "org.apache.spark" %% "spark-hive" % "2.3.0"

assemblyMergeStrategy in assembly := {

  case path if path.contains("META-INF/services") => MergeStrategy.concat

  case PathList("META-INF", xs @ _*) => MergeStrategy.discard

  case x => MergeStrategy.first

}