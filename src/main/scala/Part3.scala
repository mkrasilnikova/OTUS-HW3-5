import model.TaxiRide
import org.apache.spark.sql.functions.{col, count, date_format, date_trunc, max, mean, min, round, to_timestamp, to_utc_timestamp}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object Part3 {

  def main(args: Array[String]): Unit = {
    import ReadWriteUtils._
    import model._

    implicit val spark = SparkSession
      .builder()
      .appName("HW3-part3")
      .config("spark.master", "local")
      .getOrCreate()

    val taxiFactsDF: DataFrame = readParquet("src/main/resources/yellow_taxi_jan_25_2018")

    import spark.implicits._

    val taxiFactsDS: Dataset[TaxiRide] =
      taxiFactsDF
        .as[TaxiRide]

    val result: DataFrame = processTaxiData(taxiFactsDS)
    result.printSchema()
    result.show()
    writeToPostgres(result)

  }

  def processTaxiData(taxiFactsDS: Dataset[TaxiRide]): DataFrame = {
    taxiFactsDS
      .filter(col("trip_distance") > 0)
      .groupBy(date_format(to_utc_timestamp(col("tpep_pickup_datetime"), "GMT"), "HH").as("hour_of_the_day"))
      .agg(
        count("*").as("total_trips"),
        round(min("trip_distance"), 2).as("min_distance"),
        round(mean("trip_distance"), 2).as("mean_distance"),
        round(max("trip_distance"), 2).as("max_distance")
      )
      .orderBy(col("total_trips").desc)
  }

}


