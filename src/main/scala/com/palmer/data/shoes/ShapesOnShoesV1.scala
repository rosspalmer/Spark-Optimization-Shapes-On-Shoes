package com.palmer.data.shoes

import org.apache.spark.sql.{Dataset, SparkSession}

object ShapesOnShoesV1 extends App {

  lazy val spark: SparkSession = SparkSession.builder().getOrCreate()
  import spark.implicits._

  if (args.length != 2) throw new IllegalArgumentException("Must have two arguments")
  val purchasePath = args(0); val summaryPath = args(1)

  // Load customer purchases dataset and encode as case class
  val purchases = spark.read
                       .parquet(purchasePath)
                       .as[CustomerPurchase]

  // Run transformation function
  val summaries = transformPurchases(purchases)

  // Write to provided path
  summaries.write.parquet(summaryPath)

  def transformPurchases(purchases: Dataset[CustomerPurchase]): Dataset[CustomerSummary] = {

    purchases.groupByKey(_.customer_id).mapGroups {

      case (customer_id: Long, purchases: Iterator[CustomerPurchase]) =>

        // Sorting full set of purchases by rating descending for future purposes
        val p = purchases.toSeq.sortBy(_.shoe_rating)

        // Sort names by count in reverse
        val names = p.groupBy(_.customer_name).map {
          case (name, list) => (name, list.length)
        }.toSeq.sortBy(_._2).reverse

        // Avoid running `contains` twice for circle shoe logic
        val starShoe: Option[CustomerPurchase] = p.find(_.shoe_description.contains("star"))

        // Populate summary information using sorted purchases sequence
        CustomerSummary(
          customer_id = customer_id,
          customer_name = names.head._1,
          name_variants = if (names.length > 1) names.map(_._1).toSet else Set.empty,
          first_purchase_date = p.map(_.purchase_date).min,
          total_purchases = p.length,
          average_price = p.map(_.shoe_price).sum / p.length,
          best_shoe = p.head,
          worst_shoe = p.last,
          best_star_shoe = starShoe,
          circle_lover_designs = if (starShoe.isEmpty) Some(
            p.map(_.shoe_description).filter(_.contains("circle")).toSet
          ) else None
        )

    }
  }

}
