package com.palmer.data.shoes

import org.apache.spark.sql.Dataset

import java.sql.Date

object TransformerV1 extends TransformFunction {

  def transformPurchases(purchases: Dataset[CustomerPurchase]): Dataset[CustomerSummary] = {

    import purchases.sparkSession.implicits._
    implicit val dateOrdering = new Ordering[Date] {
      override def compare(x: Date, y: Date): Int = x.compareTo(y)
    }

    // Create anonymous function for resolving group of CustomerPurchase
    // objects by customer ID long into final CustomerSummary object
    val groupFunction: (Long, Iterator[CustomerPurchase]) => CustomerSummary = {

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
          // Only include lower count names as variants
          name_variants = if (names.length > 1) names.map(_._1).slice(1, names.length).toSet else Set.empty,
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

    purchases.groupByKey(_.customer_id).mapGroups(groupFunction)

  }

}