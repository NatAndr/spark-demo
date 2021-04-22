package org.example

import org.apache.spark.sql.QueryTest.checkAnswer
import org.apache.spark.sql.Row
import org.apache.spark.sql.test.SharedSparkSession

class DataFrameDemoSharedSparkSessionTest extends SharedSparkSession {

  test("join - processTaxiData") {
    val taxiDF      = Utils.readParquet("src/main/resources/data/yellow_taxi_jan_25_2018")
    val taxiZonesDF = Utils.readCsv("src/main/resources/data/taxi_zones.csv")

    val actual = DataFrameDemo.processTaxiData(taxiDF, taxiZonesDF)

    checkAnswer(
      actual,
      Row("Manhattan", 296527) ::
        Row("Queens", 13819) ::
        Row("Brooklyn", 12672) ::
        Row("Unknown", 6714) ::
        Row("Bronx", 1589) ::
        Row("EWR", 508) ::
        Row("Staten Island", 64) :: Nil
    )

  }

}
