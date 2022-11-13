package com.palmer.data.shoes

import org.apache.spark.sql.functions.{explode, lit, udf}
import org.apache.spark.sql.{Dataset, SparkSession}

import java.sql.Date
import scala.util.Random
import scala.math.round

object RAND {

  val r = Random
  val BASE_PROBABILITIES = Seq(
    "star" -> 0.2,
    "circle" -> 0.2,
    "square" -> 0.4,
    "triangle" -> 0.4,
    "rhombus" -> 0.3,
    "hexagon" -> 0.5,
    "crescent" -> 0.1,
    "pentagon" -> 0.3
  )

  val COLORS = Seq("red", "blue", "green", "yellow", "orange", "purple")
  def getRandomColor(): String = COLORS(r.nextInt(COLORS.size))

  def getRandomString(n: Int): String = r.alphanumeric.take(n).mkString

  val startTimestamp: Long = 1577904392000L // 2020-01-01
  val endTimestamp: Long = 1672598792000L // 2023-01-01
  val timestampDiff: Long = endTimestamp - startTimestamp

  def getRandomDate(): Date = {
    val randomDiff = r.nextLong(timestampDiff)
    val randomTimestamp = startTimestamp + randomDiff
    new Date(randomTimestamp)
  }

}

case class CustomerInfo(customerId: Long, name: String) {

  val VARIATION_PROBABILITY = 0.1

  // FIXME is cutting out too many characters
  def getNameWithPossibleVariation(): String = {
    if (RAND.r.nextDouble() > VARIATION_PROBABILITY) {
      name
    } else {
      val replacement = RAND.r.nextInt(name.length - 1)
      name.substring(0, replacement - 1) + RAND.getRandomString(1) + name.substring(replacement + 1)
    }
  }

}

object DataGenerator extends App {

  def generateCustomerInfo(customerId: Long): CustomerInfo= {
    val firstNameLength = 3 + RAND.r.nextInt(10)
    val lastNameLength = 3 + RAND.r.nextInt(10)
    CustomerInfo(
      customerId,
      s"${RAND.getRandomString(firstNameLength)} ${RAND.getRandomString(lastNameLength)}"
    )
  }

  def generateShoeDescription(probabilityOverride: Map[String, Double] = Map.empty): String = {

    // Generate list of shapes by a random check
    // against the final shape probability
    val shapes: Seq[String] = RAND.BASE_PROBABILITIES
      .map {
        case (shape, prob) =>
          val finalProb = probabilityOverride.getOrElse(shape, prob)
          if (finalProb > RAND.r.nextDouble()) Some(shape) else None
      }
      .filter(_.isDefined)
      .map(_.get)

    // Reduce shape list and include random color description
    if (shapes.isEmpty) RAND.getRandomColor() else {
      shapes.map(shape => s"${RAND.getRandomColor()} $shape").mkString(" ")
    }

  }

  def generatePurchase(info: CustomerInfo, circleLover: Boolean): CustomerPurchase = {

    // Override default probabilities for special "no star" circle lovers
    val shoeDescription = if (circleLover) generateShoeDescription(
      Map("circle" -> 0.6, "star" -> 0.0)
    ) else generateShoeDescription()

    // Generate other values using uniform distribution
    val shoePrice = RAND.r.nextDouble() * 40.0 + 20.0
    val shoeRating = RAND.r.nextDouble()

    CustomerPurchase(
      customer_id = info.customerId,
      customer_name = info.getNameWithPossibleVariation(),
      purchase_date = RAND.getRandomDate(),
      shoe_description = shoeDescription,
      shoe_price = shoePrice,
      shoe_rating = shoeRating
    )

  }

  def generatePurchaseDataset(implicit spark: SparkSession, numberCustomers: Long, avgShoeCount: Int,
                              circleLoversPct: Double): Dataset[CustomerPurchase] = {

    import spark.implicits._

    val SHOE_NUM_RANGE = 3

    // Pack majority of data generation into Spark udf with returns Seq[CustomerPurchase].
    // Total count of returned seq of data is based on defined average +-3
    val purchaseUDF = udf {

      (customerId: Long) =>

        val customerInfo = generateCustomerInfo(customerId)
        val numShoes: Long = Seq(
          1, round((avgShoeCount - SHOE_NUM_RANGE) + RAND.r.nextDouble() * 2 * SHOE_NUM_RANGE)
        ).max
        val circleLover: Boolean = RAND.r.nextDouble() <= circleLoversPct

        (1L to numShoes).map(_ => generatePurchase(customerInfo, circleLover))

    }

    spark.range(numberCustomers)
         .withColumn("purchases", explode(purchaseUDF($"id")))
         .select("purchases.*")
         .as[CustomerPurchase]

  }


}
