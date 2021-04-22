package org.example

import java.util.Properties

import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.example.Model.TaxiRide

object DatasetDemo {

  /**
    * Основная инструкция задание 3
    * Загрузить данные в DataSet из файла с фактическими данными поездок в Parquet
    * (src/main/resources/data/yellow_taxi_jan_25_2018). С помощью DSL и lambda построить таблицу, которая покажет.
    * Как происходит распределение поездок по дистанции? Результат вывести на экран и записать в бд Постгрес
    * (докер в проекте). Для записи в базу данных необходимо продумать и также приложить инит sql файл со структурой.
    *
    * (Пример: можно построить витрину со следующими колонками: общее количество поездок, среднее расстояние,
    * среднеквадратическое отклонение, минимальное и максимальное расстояние)
    *
    * Результат: В консоли должны появиться данные с результирующей таблицей, в бд должна появиться таблица
    */

  /**
    * create table script
    *
    * create table if not exists taxi_trips_info
    * (
    * id bigserial not null primary key,
    * trips_count bigint,
    * avg_distance numeric,
    * standard_deviation numeric,
    * min_distance numeric,
    * max_distance numeric,
    * created_at timestamp with time zone not null default current_timestamp
    * );
    */

  val driver   = "org.postgresql.Driver"
  val url      = "jdbc:postgresql://localhost:5432/otus"
  val user     = "docker"
  val password = "docker"

  def processTaxiData(taxiDF: DataFrame)(implicit spark: SparkSession): Dataset[Row] = {
    import spark.implicits._
    taxiDF
      .as[TaxiRide]
      .filter(_.trip_distance.nonEmpty)
      .filter(_.trip_distance.get > 0)
      .agg(
        count($"trip_distance") as "trips_count",
        avg($"trip_distance") as "avg_distance",
        stddev($"trip_distance") as "standard_deviation",
        min($"trip_distance") as "min_distance",
        max($"trip_distance") as "max_distance"
      )
      .as("agg")
  }

  def writeToTable(
      dataFrame: DataFrame,
      properties: Properties,
      url: String,
      table: String
  ): Unit = {
    dataFrame.write
      .mode(SaveMode.Append)
      .jdbc(url, table, properties)
  }

  def main(args: Array[String]): Unit = {
    implicit val spark: SparkSession = SparkSession
      .builder()
      .appName("DataSet Demo")
      .config("spark.master", "local")
      .getOrCreate()

    val taxiDF = Utils.readParquet("src/main/resources/data/yellow_taxi_jan_25_2018")

    val dataFrame = processTaxiData(taxiDF)

    dataFrame.show()

    val properties = new Properties()
    properties.setProperty("user", user)
    properties.setProperty("password", password)
    properties.setProperty("driver", driver)

    writeToTable(dataFrame, properties, url, "taxi_trips_info")

    spark.stop()
  }
}
