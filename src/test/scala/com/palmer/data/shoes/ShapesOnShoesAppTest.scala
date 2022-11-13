package com.palmer.data.shoes

import org.apache.spark.sql.Dataset
import org.scalatest.matchers.must.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.time.LocalDateTime
import java.time.temporal.{ChronoUnit}


class ShapesOnShoesAppTest extends AnyWordSpec with Matchers with SparkTest {

  "ShapesOnShoesApp" when {

    // Create anonymous classes with V1 / V2 trait and easy transform function
    val runnerV1 = new {} with V1 {
      def transform(df: Dataset[CustomerPurchase]): Dataset[CustomerSummary] = transformPurchases(df)
    }
    val runnerV2 = new {} with V2 {
      def transform(df: Dataset[CustomerPurchase]): Dataset[CustomerSummary] = transformPurchases(df)
    }

    "running on randomly generated dataset" should {

      val purchases = DataGenerator.generatePurchaseDataset(spark, 100, 10, 0.4)
        .persist()
      println(s"Purchases total count: ${purchases.count()}")

      "work properly using V1 transform" in {

        // TODO add boundry assertions
        val summaryV1 = runnerV1.transform(purchases)
        summaryV1.show()

      }

      "work properly using V2 transform" in {

        // TODO add boundry assertions
        val summaryV2 = runnerV2.transform(purchases)
        summaryV2.show()

      }


    }
  }
}
