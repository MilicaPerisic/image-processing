name := "test-spark"

version := "0.1"

scalaVersion := "2.13.6"

val sparkVersion = "3.2.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % sparkVersion % "compile",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion % "compile",
  "org.apache.spark" %% "spark-avro" % sparkVersion % "compile",
  "org.apache.spark" %% "spark-streaming" % sparkVersion % "compile"

)

libraryDependencies += "org.apache.spark" %% "spark-avro" % "3.2.0"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "3.2.0" % "provided"