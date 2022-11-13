package com.palmer.data.shoes

import org.apache.spark.sql.SparkSession

trait SparkTest {

  implicit val spark: SparkSession = SparkSession.builder().master("local").getOrCreate()

}
