package com.palmer.data.shoes

import org.scalatest.matchers.must.Matchers
import org.scalatest.wordspec.AnyWordSpec

class DataGeneratorTest extends AnyWordSpec with Matchers {

  "DataGenerator" when {

    "generating a shoe description" should {

      "create a random shoe with no modifier" in {

        val randomShoe = DataGenerator.generateShoeDescription()

        randomShoe.length must be > 0
        println(s"Random: $randomShoe")

      }

      "Guaranteed star shoe using override" in {

        val starShoe = DataGenerator.generateShoeDescription(Map("star" -> 1.0))

        starShoe.length must be > 0
        starShoe.split("\\s") must contain("star")

      }

      "Guaranteed no star shoe using override" in {

        val starShoe = DataGenerator.generateShoeDescription(Map("star" -> 0.0))

        starShoe.length must be > 0
        starShoe.split("\\s") must not contain("star")

      }

    }

  }

}
