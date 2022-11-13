package com.palmer.data.shoes

import org.scalatest.matchers.must.Matchers
import org.scalatest.wordspec.AnyWordSpec

class DataGeneratorTest extends AnyWordSpec with Matchers {

  "RAND random generator" when {

    "generating a date" should {

      "create a date within 2020-01-01 and 2023-01-02" in {

        val randomDate = RAND.getRandomDate()

        // TODO add bounded assertion
        println(randomDate)

      }

    }

  }

  "DataGenerator" when {

    "generating customer info" should {

      "create a random string name" in {

        val custId = 1020304L
        val info = DataGenerator.generateCustomerInfo(custId)

        info.customerId must equal (custId)

        val nameParts = info.name.split("\\s")
        nameParts must have length 2
        nameParts(0).length must be >= 3
        nameParts(1).length must be >= 3

        println(s"Generated name: ${info.name}")

      }

    }

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
