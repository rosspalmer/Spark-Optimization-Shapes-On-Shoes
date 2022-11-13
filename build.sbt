
ThisBuild / name := "Shapes-On-Shoes"
ThisBuild / scalaVersion := "2.13.10"
ThisBuild / organization := "com.palmer.data"
ThisBuild / version      := "0.2.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.3.1",
  "org.apache.spark" %% "spark-sql" % "3.3.1",
  "org.scalactic" %% "scalactic" % "3.2.14",
  "org.scalatest" %% "scalatest" % "3.2.14" % "test"
)