package com.palmer.data.shoes

import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Dataset, Encoder, SparkSession}

import java.sql.Date


object ShapesOnShoesApp extends App {

  lazy val spark: SparkSession = SparkSession.builder().getOrCreate()
  import spark.implicits._

  if (args.length != 3) throw new IllegalArgumentException("Must have three arguments")
  val version = args(0)
  val purchasePath = args(1)
  val summaryPath = args(2)

  // Load customer purchases dataset and encode as case class
  val purchases = spark.read
    .parquet(purchasePath)
    .as[CustomerPurchase]

  // Get transformation function based on version argument
  val transformer: TransformFunction = version match {
    case "v1" => TransformerV1
    case "v2" => TransformerV2
  }

  // Aggregate purchases data into customer summaries
  val summaries = transformer.transformPurchases(purchases)

  // Write to provided path
  summaries.write.parquet(summaryPath)

}

trait TransformFunction {

  def transformPurchases(purchases: Dataset[CustomerPurchase]): Dataset[CustomerSummary]

}
