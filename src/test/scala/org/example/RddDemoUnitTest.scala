package org.example

import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec

class RddDemoUnitTest extends AnyFlatSpec {

  implicit val spark: SparkSession = SparkSession
    .builder()
    .config("spark.master", "local")
    .appName("Test for RDD Demo")
    .getOrCreate()

  it should "upload and process data" in {
    val taxiDF = Utils.readParquet("src/main/resources/data/yellow_taxi_jan_25_2018")

    val actual = RddDemo
      .processTaxiData(taxiDF)
      .take(10)

    assert(actual(0) == "19 22121")
    assert(actual(1) == "20 21598")
    assert(actual(2) == "22 20884")
    assert(actual(3) == "21 20318")
  }

}
