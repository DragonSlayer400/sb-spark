name := "data_mart"

version := "1.0"

scalaVersion := "2.11.12"
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.7"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.7"
libraryDependencies += "org.elasticsearch" %% "elasticsearch-spark-20" % "7.9.2"
libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "2.4.3"
libraryDependencies += "org.postgresql" % "postgresql" % "42.2.16"
