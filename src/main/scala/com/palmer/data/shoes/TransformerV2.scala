package com.palmer.data.shoes

import org.apache.spark.sql.{Dataset, Encoder}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.expressions.Aggregator

import java.sql.Date


object TransformerV2 extends TransformFunction {

  def transformPurchases(purchases: Dataset[CustomerPurchase]): Dataset[CustomerSummary] = {

    import purchases.sparkSession.implicits._

    val aggFunction = new PurchaseAggregator().toColumn

    purchases.groupByKey(_.customer_id)
      .agg(aggFunction.name("summary"))
      .select($"summary".as[CustomerSummary])

  }

}

// Buffer class is optimized to retain as little data as possible
// before `finish` resolves the aggregation (map for names)
case class SummaryBuffer(
                          customerId: Long,
                          names: Seq[String],
                          firstPurchase: Date,
                          totalCount: Long,
                          averagePrice: Double,
                          best: CustomerPurchase,
                          worst: CustomerPurchase,
                          star: Option[CustomerPurchase],
                          circles: Option[Set[String]]
                        )

class PurchaseAggregator extends Aggregator[CustomerPurchase, Option[SummaryBuffer], CustomerSummary] {

  override def zero: Option[SummaryBuffer] = None

  override def reduce(b: Option[SummaryBuffer], a: CustomerPurchase): Option[SummaryBuffer] = {

    val isStar = a.shoe_description.contains("star")
    val circleLover = !isStar && a.shoe_description.contains("circle")

    val newBuffer = SummaryBuffer(
      customerId = a.customer_id,
      names = Seq(a.customer_name),
      firstPurchase = a.purchase_date,
      totalCount = 1L,
      averagePrice = a.shoe_price,
      best = a,
      worst = a,
      star = if (isStar) Some(a) else None,
      circles = if (circleLover) Some(Set(a.shoe_description)) else None
    )

    // IF non-zero b case merge two buffers, else return newBuffer
    b match {
      case Some(other) => Some(mergeBuffers(other, newBuffer))
      case None => Some(newBuffer)
    }

  }

  override def merge(b1: Option[SummaryBuffer], b2: Option[SummaryBuffer]): Option[SummaryBuffer] = {

    (b1, b2) match {
      case (Some(s1), Some(s2)) => Some(mergeBuffers(s1, s2))
      case (Some(s), None) => Some(s)
      case (None, Some(s)) => Some(s)
      case _ => None
    }

  }

  def mergeBuffers(a: SummaryBuffer, b: SummaryBuffer): SummaryBuffer = {

    // First purchase is min of two dates (simple logic)
    val firstPurchase: Date = if (a.firstPurchase.compareTo(b.firstPurchase) < 0) a.firstPurchase else b.firstPurchase

    // Total count and average price can be calculated
    val totalCount = a.totalCount + b.totalCount
    val averagePrice = (a.totalCount * a.averagePrice + b.totalCount * b.averagePrice) / totalCount

    // Only keep circle data if there are no star shoes
    val circleLoverData: Option[Set[String]] = {
      if (a.star.isDefined || b.star.isDefined) {
        None
      } else {
        (a.circles, b.circles) match {
          case (Some(c1), Some(c2)) => Some(c1 ++ c2)
          case (Some(c), None) => Some(c)
          case (None, Some(c)) => Some(c)
          case _ => None
        }
      }
    }

    SummaryBuffer(

      customerId = a.customerId,
      names = a.names ++ b.names,
      firstPurchase = firstPurchase,
      totalCount = totalCount,
      averagePrice = averagePrice,

      // Note, merge function is biased towards `b` side
      best = if (a.best.shoe_rating > b.best.shoe_rating) a.best else b.best,
      worst = if (a.worst.shoe_rating < b.worst.shoe_rating) a.worst else b.worst,
      star = (a.star, b.star) match {
        case (Some(aStar), Some(bStar)) => if (aStar.shoe_rating > bStar.shoe_rating) Some(aStar) else Some(bStar)
        case (Some(star), None) => Some(star)
        case (None, Some(star)) => Some(star)
        case _ => None
      },

      circles = circleLoverData

    )

  }

  override def finish(reduction: Option[SummaryBuffer]): CustomerSummary = {

    val buf = reduction.get
    val sortedNames: Seq[String] = buf.names
      .groupBy(n => n).toSeq
      .sortBy(_._2.length).reverse
      .map(_._1)

    CustomerSummary(
      customer_id = buf.customerId,
      customer_name = sortedNames.head,
      // Only include lower count names as variants
      name_variants = if (sortedNames.length > 1) sortedNames.slice(1, sortedNames.length).toSet else Set.empty,
      first_purchase_date = buf.firstPurchase,
      total_purchases = buf.totalCount,
      average_price = buf.averagePrice,
      best_shoe = buf.best,
      worst_shoe = buf.worst,
      best_star_shoe = buf.star,
      circle_lover_designs = buf.circles
    )

  }

  override def bufferEncoder: Encoder[Option[SummaryBuffer]] = ExpressionEncoder[Option[SummaryBuffer]]

  override def outputEncoder: Encoder[CustomerSummary] = ExpressionEncoder[CustomerSummary]

}