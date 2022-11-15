package com.palmer.data.shoes

import org.apache.spark.sql.{Dataset, Encoder, Encoders}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.expressions.Aggregator

import java.sql.Date


object TransformerV2 extends TransformFunction {

  def transformPurchases(purchases: Dataset[CustomerPurchase]): Dataset[CustomerSummary] = {

    import purchases.sparkSession.implicits._

    // Initialize aggregators and create column objects
    val info = new InfoAggregator().toColumn
    val first = new DateAggregator().toColumn
    val stats = new StatsAggregator().toColumn
    val best = new RatingsAggregator(true).toColumn
    val worst = new RatingsAggregator(false).toColumn
    val bestStar = new RatingsAggregator(true, Option("star")).toColumn
    val circles = new CircleLoversAggregator().toColumn

    purchases.groupByKey(_.customer_id)
      .agg(
        info.name("info"),
        first.name("first_purchase_date"),
        stats.name("stats"),
        best.name("best_shoe"),
        worst.name("worst_shoe"),
        bestStar.name("best_star_shoe"),
        circles.name("circle_lover_designs")
      )
      .select(
        $"info".getField("_1").name("customer_id"),
        $"info".getField("_2").name("customer_name"),
        $"info".getField("_3").name("name_variants"),
        $"first_purchase_date",
        $"stats".getField("_1").name("total_purchases"),
        $"stats".getField("_2").name("average_price"),
        $"best_shoe", $"worst_shoe",
        $"best_star_shoe", $"circle_lover_designs"
      ).as[CustomerSummary]

  }

}


class InfoAggregator extends Aggregator[CustomerPurchase, (Long, Seq[String]), (Long, String, Set[String])] {

  override def zero: (Long, Seq[String]) = (-1L, Seq.empty)

  override def reduce(b: (Long, Seq[String]), a: CustomerPurchase): (Long, Seq[String]) = b._1 match {
    case -1L => (a.customer_id, Seq(a.customer_name))
    case _ => (b._1, b._2 :+ a.customer_name)
  }

  override def merge(b1: (Long, Seq[String]), b2: (Long, Seq[String])):
    (Long, Seq[String]) = {
      (b1._1, b2._1) match {
        case (-1, -1) => b1
        case (_, -1) => b1
        case (-1, _) => b2
        case _ => (b1._1, b1._2 ++ b2._2)
      }
  }

  override def finish(reduction: (Long, Seq[String])): (Long, String, Set[String]) = {


    // Sort by frequency descending
    val namesSorted = reduction._2.groupBy(n => n).toSeq
                                  .sortBy(_._2.length).reverse
                                  .map(_._1)

    (
      reduction._1, // Customer ID
      namesSorted.head, // Top occurring name
      // Variants if present
      if (namesSorted.length > 1) namesSorted.slice(1, namesSorted.length).toSet else Set.empty
    )

  }

  override def bufferEncoder: Encoder[(Long, Seq[String])] = Encoders.tuple(
    ExpressionEncoder[Long], ExpressionEncoder[Seq[String]]
  )

  override def outputEncoder: Encoder[(Long, String, Set[String])] = Encoders.tuple(
    ExpressionEncoder[Long], Encoders.STRING, ExpressionEncoder[Set[String]]
  )
}


class DateAggregator extends Aggregator[CustomerPurchase, Option[Date], Date] {
  override def zero: Option[Date] = None

  override def reduce(b: Option[Date], a: CustomerPurchase): Option[Date] = b match {
    case None => Some(a.purchase_date)
    case Some(d) => if (a.purchase_date.compareTo(d) < 0) Some(a.purchase_date) else b
  }

  override def merge(b1: Option[Date], b2: Option[Date]): Option[Date] = (b1, b2) match {
    case (Some(d1), Some(d2)) => if (d1.compareTo(d2) < 0) b1 else b2
    case (Some(d), None) => b1
    case (None, Some(d)) => b2
    case _ => None
  }

  override def finish(reduction: Option[Date]): Date = reduction.get
  override def bufferEncoder: Encoder[Option[Date]] = ExpressionEncoder[Option[Date]]
  override def outputEncoder: Encoder[Date] = Encoders.DATE

}

class StatsAggregator extends Aggregator[CustomerPurchase, (Long, Double), (Long, Double)] {

  override def zero: (Long, Double) = (0, 0.0)

  override def reduce(b: (Long, Double), a: CustomerPurchase): (Long, Double) = b._1 match {
    case 0 => (1, a.shoe_rating)
    case _ =>
      val total = b._1 + 1
      (
        total,
        (b._1 * b._2 + a.shoe_price) / total // New price average
      )
  }

  override def merge(b1: (Long, Double), b2: (Long, Double)): (Long, Double) = (b1._1, b2._1) match {
    case (0, 0) | (_, 0) => b1
    case (0, _) => b2
    case _ =>
      val total = b1._1 + b2._1
      (
        total,
        (b1._1 * b1._2 + b2._1 * b2._2) / total // New average price
      )
  }

  override def finish(reduction: (Long, Double)): (Long, Double) = reduction
  override def bufferEncoder: Encoder[(Long, Double)] = Encoders.tuple(ExpressionEncoder[Long], ExpressionEncoder[Double])
  override def outputEncoder: Encoder[(Long, Double)] = Encoders.tuple(ExpressionEncoder[Long], ExpressionEncoder[Double])

}


class RatingsAggregator(best: Boolean, designFilter: Option[String] = None)
  extends Aggregator[CustomerPurchase, Option[CustomerPurchase], Option[CustomerPurchase]] {

  val encoder = ExpressionEncoder[Option[CustomerPurchase]]

  override def zero: Option[CustomerPurchase] = None

  override def reduce(b: Option[CustomerPurchase], a: CustomerPurchase): Option[CustomerPurchase] = {

    if (designFilter.isEmpty || a.shoe_description.contains()) {

      if (b.isDefined) {

        best match {
          case true => if (a.shoe_rating > b.get.shoe_rating) Some(a) else b
          case false => if (a.shoe_rating < b.get.shoe_rating) Some(a) else b
        }

      } else Some(a)

    } else b

  }

  override def merge(b1: Option[CustomerPurchase], b2: Option[CustomerPurchase]): Option[CustomerPurchase] = {
    (b1, b2) match {
      case (Some(p1), Some(p2)) =>
        best match {
          case true => if (p1.shoe_rating > p2.shoe_rating) b1 else b2
          case false => if (p1.shoe_rating < p2.shoe_rating) b1 else b2
        }
      case (Some(p), None) => b1
      case (None, Some(p)) => b2
      case _ => None
    }
  }

  override def finish(reduction: Option[CustomerPurchase]): Option[CustomerPurchase] = reduction
  override def bufferEncoder: Encoder[Option[CustomerPurchase]] = encoder
  override def outputEncoder: Encoder[Option[CustomerPurchase]] = encoder

}


class CircleLoversAggregator extends Aggregator[CustomerPurchase, Option[Set[String]], Option[Set[String]]] {

  val encoder = ExpressionEncoder[Option[Set[String]]]

  // Will use None to represent star shoe present
  override def zero: Option[Set[String]] = Some(Set.empty)

  override def reduce(b: Option[Set[String]], a: CustomerPurchase): Option[Set[String]] = b match {
    case None => None
    case Some(c) => if (a.shoe_description.contains("star")) None else {
      if (a.shoe_description.contains("circle")) Some(c + a.shoe_description) else b
    }
  }

  override def merge(b1: Option[Set[String]], b2: Option[Set[String]]): Option[Set[String]] = {
    if (b1.isEmpty || b2.isEmpty) None else {
      (b1.get.nonEmpty, b2.get.nonEmpty) match {
        case (true, true) => Some(b1.get ++ b2.get)
        case (true, false) => b1
        case _ => b2
      }
    }
  }

  override def finish(reduction: Option[Set[String]]): Option[Set[String]] = reduction
  override def bufferEncoder: Encoder[Option[Set[String]]] = encoder
  override def outputEncoder: Encoder[Option[Set[String]]] = encoder

}