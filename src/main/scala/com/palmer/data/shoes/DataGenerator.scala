package com.palmer.data.shoes

import java.sql.Date
import scala.util.Random

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

case class CustomerInfo(customerId: Long, name: String)

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


}
