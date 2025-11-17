ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.16"

val sparkVersion = "4.0.1"

lazy val root = (project in file("."))
  .settings(
    name := "Analyse-et-Prevision-Pollution",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion,
      "org.apache.spark" %% "spark-sql"  % sparkVersion,
      "org.apache.spark" %% "spark-mllib" % sparkVersion
  ))