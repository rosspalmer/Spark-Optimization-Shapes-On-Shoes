package com.palmer.data.shoes

import org.apache.spark.sql.Dataset
import org.scalatest.matchers.must.Matchers
import org.scalatest.wordspec.AnyWordSpec


class ShapesOnShoesAppTest extends AnyWordSpec with Matchers with SparkTest {

  "ShapesOnShoesApp" when {

    // Create anonymous class with V1 trait and easy transform function
    val runnerV1 = new {} with V1 {
      def transform(df: Dataset[CustomerPurchase]): Dataset[CustomerSummary] = transformPurchases(spark, df)
    }

    "running on randomly generated dataset" should {

      val purchases = DataGenerator.generatePurchaseDataset(spark, 10, 5, 0.4)
        .persist()

      "properly transform using V1 functions" in {

        // TODO add checks
        val summary = runnerV1.transform(purchases)
        summary.show()

      }


    }
  }
}
