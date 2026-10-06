ThisBuild / scalaVersion := "2.13.18"

name := "real-time-shipment-tracking"
version := "1.0.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "4.0.1",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "4.0.1",
  "org.apache.kafka" % "kafka-clients" % "4.0.1",
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.19.2"
)

Compile / run / fork := true
Compile / run / javaOptions ++= Seq("-Xms512m", "-Xmx2g")
