
ThisBuild / name := "Shapes-On-Shoes"
ThisBuild / scalaVersion := "2.12.15"
ThisBuild / organization := "com.palmer.data"
ThisBuild / version      := "0.2.2"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.3.0",
  "org.apache.spark" %% "spark-sql" % "3.3.0",
  "org.scalactic" %% "scalactic" % "3.2.14",
  "org.scalatest" %% "scalatest" % "3.2.14" % "test"
)