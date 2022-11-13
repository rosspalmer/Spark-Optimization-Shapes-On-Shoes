package com.palmer.data.shoes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{max, min}
import org.scalatest.matchers.must.Matchers
import org.scalatest.wordspec.AnyWordSpec

class DataGeneratorTest extends AnyWordSpec with Matchers {

  implicit val spark: SparkSession = SparkSession.builder().master("local").getOrCreate()
  import spark.implicits._

  "RAND random generator" when {

    "generating a date" should {

      "create a date within 2020-01-01 and 2023-01-02" in {

        val randomDate = RAND.getRandomDate()

        // TODO add bounded assertion
        println(randomDate)

      }

    }

    "generating a string" should {

      val fourLetter = RAND.getRandomString(4)

      "have proper length" in {

        fourLetter.length must equal (4)

      }

      "create a single character" in {

        val single = RAND.getRandomString(1)
        single.length must equal (1)

      }

    }

  }

  "CustomerInfo case class" when {

    "providing name with possible variant" should {

      val info = CustomerInfo(123L, "hello dude")
      val names: Seq[String] = (1 to 200).map(_ => info.getNameWithPossibleVariation())

      "return same length names" in {

        val nameLength = names.head.length
        for (name <- names) {

          name must have length nameLength

        }

      }

      "return ~10% variants" in {

        names must have length 200

        val nameCounts: Seq[(String, Int)] = names.groupBy(n => n)
                                                  .map(n => (n._1, n._2.length)).toSeq
                                                  .sortBy(_._2).reverse

        nameCounts.length must be >= 2

        val topNameCount = nameCounts.head._2
        println(topNameCount)

        // Check for 95% (190) - 70% (140) result range
        topNameCount must be <= 190
        topNameCount must be >= 140


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

    "generating a purchase dataset" should {

      val NUM_CUSTOMERS = 10
      val AVG_SHOES = 2
      val CIRCLE_LOVE = 0.5

      val df = DataGenerator.generatePurchaseDataset(spark, NUM_CUSTOMERS, AVG_SHOES, CIRCLE_LOVE)
                            .persist()

      "have proper absolute max/min bounds" in {

        val bounds = df.select(
          min($"customer_id").alias("min_id"), max($"customer_id").alias("max_id"),
          min($"purchase_date").alias("min_date"), max($"purchase_date").alias("max_date"),
          min($"shoe_price").alias("min_date"), max($"shoe_price").alias("max_date"),
          min($"purchase_date").alias("min_date"), max($"purchase_date").alias("max_date"),
        )

        bounds.show

      }

    }

  }

}
