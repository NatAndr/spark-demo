package org.example

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.example.Model.TaxiRide

object RddDemo {

  /**
    * Основная инструкция задание 2
    * Загрузить данные в RDD из файла с фактическими данными поездок в Parquet
    * (src/main/resources/data/yellow_taxi_jan_25_2018). С помощью lambda построить таблицу, которая покажет
    * В какое время происходит больше всего вызовов. Результат вывести на экран и в txt файл c пробелами.
    *
    * Результат: В консоли должны появиться данные с результирующей таблицей, в файловой системе должен появиться файл.
    */

  def processTaxiData(taxiDF: DataFrame)(implicit spark: SparkSession): RDD[String] = {
    import spark.implicits._
    taxiDF
      .as[TaxiRide]
      .rdd
      .filter(_.tpep_pickup_datetime.nonEmpty)
      .map(t =>
        LocalDateTime
          .parse(t.tpep_pickup_datetime.get, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
      )
      .map(t => (t.getHour, 1))
      .reduceByKey(_ + _)
      .sortBy(_._2, ascending = false)
      .map(x => s"${x._1} ${x._2}")
  }

  def main(args: Array[String]): Unit = {
    implicit val spark: SparkSession = SparkSession
      .builder()
      .appName("RDD Demo")
      .config("spark.master", "local")
      .getOrCreate()

    val taxiDF = Utils.readParquet("src/main/resources/data/yellow_taxi_jan_25_2018")
    val value  = processTaxiData(taxiDF)

    value
      .foreach(println)

    value
      .saveAsTextFile("tmp/output/topHours.txt")

    spark.stop()
  }
}
