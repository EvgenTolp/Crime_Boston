name := "Boston"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "2.4.7",
  "com.typesafe" % "config" % "1.4.1",
  "com.databricks" % "spark-csv_2.10" % "1.2.0"

)

