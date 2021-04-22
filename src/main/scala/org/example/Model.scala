package org.example

object Model {
  case class TaxiRide(
      VendorID: Option[Int],
      tpep_pickup_datetime: Option[String],
      tpep_dropoff_datetime: Option[String],
      passenger_count: Option[Int],
      trip_distance: Option[Double],
      RatecodeID: Option[Int],
      store_and_fwd_flag: Option[String],
      DOLocationID: Option[Int],
      payment_type: Option[Int],
      fare_amount: Option[Double],
      extra: Option[Double],
      mta_tax: Option[Double],
      tip_amount: Option[Double],
      tolls_amount: Option[Double],
      improvement_surcharge: Option[Double],
      total_amount: Option[Double]
  )
}
