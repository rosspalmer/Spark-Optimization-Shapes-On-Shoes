package com.palmer.data.shoes

import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Dataset, Encoder, SparkSession}

import java.sql.Date


trait ShapesOnShoesApp extends App {

  def transformPurchases(spark: SparkSession, value: Dataset[CustomerPurchase]): Dataset[CustomerSummary]

  lazy val spark: SparkSession = SparkSession.builder().getOrCreate()
  import spark.implicits._

  if (args.length != 2) throw new IllegalArgumentException("Must have two arguments")
  val purchasePath = args(0);
  val summaryPath = args(1)

  // Load customer purchases dataset and encode as case class
  val purchases = spark.read
    .parquet(purchasePath)
    .as[CustomerPurchase]

  // Run transformation function
  val summaries = transformPurchases(spark, purchases)

  // Write to provided path
  summaries.write.parquet(summaryPath)

}


trait V1 {

  def transformPurchases(spark: SparkSession, purchases: Dataset[CustomerPurchase]): Dataset[CustomerSummary] = {

    import spark.implicits._

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

    purchases.groupByKey(_.customer_id).mapGroups(groupFunction)

  }

}

object ShapesOnShoesV1App extends ShapesOnShoesApp with V1


// ---- V2: Using Spark SQL Aggregator extension ----

// Buffer class is optimized to retain as little data as possible
// before `finish` resolves the aggregation (map for names)
case class SummaryBuffer(
  customerId: Long,
  nameCount: Map[String, Long],
  firstPurchase: Date,
  totalCount: Long,
  averagePrice: Double,
  best: CustomerPurchase,
  worst: CustomerPurchase,
  star: Option[CustomerPurchase],
  circles: Option[Seq[String]]
)

class PurchaseAggregator extends Aggregator[CustomerPurchase, Option[SummaryBuffer], CustomerSummary] {

  override def zero: Option[SummaryBuffer] = None

  override def reduce(b: Option[SummaryBuffer], a: CustomerPurchase): Option[SummaryBuffer] = {

    val isStar = a.shoe_description.contains("star")
    val circleLover = !isStar && a.shoe_description.contains("circle")

    val newBuffer = SummaryBuffer(
      customerId = a.customer_id,
      nameCount = Map(a.customer_name -> 1),
      firstPurchase = a.purchase_date,
      totalCount = 1L,
      averagePrice = a.shoe_price,
      best = a,
      worst = a,
      star = if (isStar) Some(a) else None,
      circles = if (circleLover) Some(Seq(a.shoe_description) else None
    )

    // Non-zero b case
    if (b.isDefined) {



    }

    ???
  }

  override def merge(b1: Option[SummaryBuffer], b2: Option[SummaryBuffer]): Option[SummaryBuffer] = {

    (b1, b2) match {
      case (Some(s1), Some(s2)) => Some(mergeBuffers(s1, s2))
      case (Some(s), None) => Some(s)
      case (None, Some(s)) => Some(s)
      case _ => None
    }

  }

  private def mergeBuffers(a: SummaryBuffer, b: SummaryBuffer): SummaryBuffer = {

    // Merge name counts by adding `a` map to `b` map plus `a` lookup counts
    val mergedNameCount = a.nameCount + b.nameCount.map {
      case (name, count) => (name, a.nameCount.getOrElse(name, 0) + count)
    }

    // First purchase is min of two dates
    val firstPurchase: Date = Seq(a.firstPurchase, b.firstPurchase).min

    // Total count and average price can be calculated
    val totalCount = a.totalCount + b.totalCount
    val averagePrice = (a.totalCount * a.averagePrice + b.totalCount * b.averagePrice) / totalCount

    // Only keep circle data if there are no star shoes
    val circleLoverData: Option[Seq[String]] = {
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
      nameCount = mergedNameCount,
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

  override def finish(reduction: Option[SummaryBuffer]): CustomerSummary = ???

  override def bufferEncoder: Encoder[Option[SummaryBuffer]] = ExpressionEncoder[Option[SummaryBuffer]]

  override def outputEncoder: Encoder[CustomerSummary] = ExpressionEncoder[CustomerSummary]
}

trait V2 {

  def transformPurchases(spark: SparkSession, purchases: Dataset[CustomerPurchase]): Dataset[CustomerSummary] = {

    import purchases.sparkSession.implicits._

    val aggFunction = new PurchaseAggregator().toColumn

    purchases.groupByKey(_.customer_id)
             .agg(aggFunction)
             .select($"_2".as[CustomerSummary])

  }

}