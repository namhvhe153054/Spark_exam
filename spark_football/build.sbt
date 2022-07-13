ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.10"

lazy val root = (project in file("."))
  .settings(
    name := "spark_exam1"
  )
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.1.3" ,
  "org.apache.spark" %% "spark-sql" % "3.1.3"
)