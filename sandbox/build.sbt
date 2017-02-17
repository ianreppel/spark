name := "Spark Sandbox"
version := "0.0.1"
scalaVersion := "2.11.8"

val spark = "org.apache.spark"
val sparkVersion = "2.1.0"

libraryDependencies ++= Seq(spark %% "spark-core" % sparkVersion,
                            spark %% "spark-sql"  % sparkVersion)
