package org.example

import org.apache.spark.sql.QueryTest.checkAnswer
import org.apache.spark.sql.Row
import org.apache.spark.sql.test.SharedSparkSession

class DatasetDemoSharedSparkSessionTest extends SharedSparkSession {

  test("processTaxiData") {
    val taxiDF = Utils.readParquet("src/main/resources/data/yellow_taxi_jan_25_2018")

    val actual = DatasetDemo.processTaxiData(taxiDF)

    checkAnswer(
      actual,
      Row(330023, 2.733390309160238, 3.488984621482059, 0.01, 66.0) :: Nil
    )

  }

}
