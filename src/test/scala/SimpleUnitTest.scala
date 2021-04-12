import Part2.getRushHours
import ReadWriteUtils.{readParquet}
import model.TaxiRide
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec

class SimpleUnitTest extends AnyFlatSpec {


  implicit val spark = SparkSession.builder()
    .config("spark.master", "local")
    .appName("Test for Part2")
    .getOrCreate()

  it should "upload and process data" in {
    val taxiDF2 = readParquet("src/main/resources/yellow_taxi_jan_25_2018")

    import spark.implicits._
    val actualDistribution = getRushHours(taxiDF2.as[TaxiRide].rdd)
      .collect()

    assert(actualDistribution === Array("19 22121","20 21598","22 20884","21 20318","23 19528","09 18867","18 18664","16 17843","15 17483","10 16840","17 16160","14 16082","13 16001","12 15564","08 15445","11 15348","00 14652","07 8600","01 7050","02 3978","06 3133","03 2538","04 1610","05 1586"))
  }

}
