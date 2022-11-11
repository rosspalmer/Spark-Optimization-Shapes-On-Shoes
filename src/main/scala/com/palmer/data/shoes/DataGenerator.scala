package com.palmer.data.shoes

import scala.util.Random

object GeneratorConfig {

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

}


object DataGenerator extends App {

  def generateShoeDescription(probabilityOverride: Map[String, Double] = Map.empty): String = {

    // Generate list of shapes by a random check
    // against the final shape probability
    val shapes: Seq[String] = GeneratorConfig.BASE_PROBABILITIES
      .map {
        case (shape, prob) =>
          val finalProb = probabilityOverride.getOrElse(shape, prob)
          if (finalProb > GeneratorConfig.r.nextDouble()) Some(shape) else None
      }
      .filter(_.isDefined)
      .map(_.get)

    // Reduce shape list and include random color description
    if (shapes.isEmpty) GeneratorConfig.getRandomColor() else {
      shapes.map(shape => s"${GeneratorConfig.getRandomColor()} $shape").mkString(" ")
    }

  }

}
