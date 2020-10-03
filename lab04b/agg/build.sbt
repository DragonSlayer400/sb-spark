name := "agg"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.7"
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.7"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.4.7" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.4.7" % "provided"
