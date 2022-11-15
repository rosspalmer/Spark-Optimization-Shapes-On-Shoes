package com.palmer.data.shoes

import org.apache.spark.sql.Dataset
import org.scalatest.matchers.must.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.time.LocalDateTime
import java.time.temporal.{ChronoUnit}


class ShapesOnShoesAppTest extends AnyWordSpec with Matchers with SparkTest {

  "ShapesOnShoesApp" when {

    "running on randomly generated dataset" should {

      val purchases = DataGenerator.generatePurchaseDataset(spark, 100, 10, 2, 0.4)
        .persist()
      println(s"Purchases total count: ${purchases.count()}")

      "work properly using V1 transform" in {

        // TODO add boundry assertions
        val summaryV1 = TransformerV1.transformPurchases(purchases)
        summaryV1.show()

      }

      "work properly using V2 transform" in {

        // TODO add boundry assertions
        val summaryV2 = TransformerV2.transformPurchases(purchases)
        summaryV2.show()

      }


    }
  }
}
