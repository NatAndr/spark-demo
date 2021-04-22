package org.example

import org.apache.spark.sql.functions.broadcast
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object DataFrameDemo {

  /**
    * Основная инструкция задание 1
    * Загрузить данные в первый DataFrame из файла с фактическими данными поездок в Parquet
    * (src/main/resources/data/yellow_taxi_jan_25_2018). Загрузить данные во второй DataFrame из файла со справочными
    * данными поездок в csv (src/main/resources/data/taxi_zones.csv) С помощью DSL построить таблицу, которая покажет
    * какие районы самые популярные для заказов. Результат вывести на экран и записать в файл Паркет.
    *
    * Результат: В консоли должны появиться данные с результирующей таблицей, в файловой системе должен появиться файл.
    */

  def processTaxiData(taxiDF: DataFrame, taxiZonesDF: DataFrame)(implicit
      spark: SparkSession
  ): Dataset[Row] = {
    import spark.implicits._
    taxiDF
      .join(broadcast(taxiZonesDF), $"DOLocationID" === $"LocationID", "left")
      .groupBy($"Borough")
      .count()
      .orderBy($"count".desc)
  }

  def main(args: Array[String]): Unit = {
    implicit val spark: SparkSession = SparkSession
      .builder()
      .appName("DataFrame Demo")
      .config("spark.master", "local")
      .getOrCreate()

    val taxiDF      = Utils.readParquet("src/main/resources/data/yellow_taxi_jan_25_2018")
    val taxiZonesDF = Utils.readCsv("src/main/resources/data/taxi_zones.csv")

    val value = processTaxiData(taxiDF, taxiZonesDF)
    value.show()

    value.write
      .parquet("tmp/output/topBoroughs.parquet")

    spark.stop()
  }

}
