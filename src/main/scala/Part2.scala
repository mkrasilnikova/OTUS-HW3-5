import model.TaxiRide
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}


object Part2 {
  def main(args: Array[String]): Unit = {
    import ReadWriteUtils._
    import model._

    implicit val spark = SparkSession
      .builder()
      .appName("HW3-part2")
      .config("spark.master", "local")
      .getOrCreate()

    val taxiFactsDF: DataFrame = readParquet("src/main/resources/yellow_taxi_jan_25_2018")
    taxiFactsDF.printSchema()
    taxiFactsDF.show(30)

    import spark.implicits._

    val taxiFactsDS: Dataset[TaxiRide] =
      taxiFactsDF
        .as[TaxiRide]

    val taxiFactsRDD: RDD[TaxiRide] =
      taxiFactsDS.rdd

    val result: DataFrame = getRushHours(taxiFactsRDD).toDF("hour_of_the_day count")
    result.show()

    writeToFile(result, "text", "src/main/resources/rushHours.txt")
  }

  def getRushHours(taxiFactsRDD: RDD[TaxiRide]): RDD[(String)] = {
    taxiFactsRDD
      .map(x => (x.tpep_pickup_datetime.split(" ")(1).split(":")(0), 1))
      .reduceByKey(_ + _)
      .sortBy(_._2, false)
      .map(t => t._1 + " " + t._2)
  }
}
