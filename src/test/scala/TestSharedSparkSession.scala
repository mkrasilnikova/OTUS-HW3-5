import Part1.getMostPopularBoroughs
import Part3.processTaxiData
import ReadWriteUtils.{readCSV, readParquet}
import model.TaxiRide
import org.apache.spark.sql.QueryTest.checkAnswer
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.test.SharedSparkSession
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper


class TestSharedSparkSession extends SharedSparkSession {
  import testImplicits._

  test("part3 - processTaxiData") {
    // без этого используется timezon = America/Los_Angeles
    spark.conf.set("spark.sql.session.timeZone", "UTC+3")
    val taxiFactsDF: DataFrame = readParquet("src/main/resources/yellow_taxi_jan_25_2018")

    val taxiFactsDS: Dataset[TaxiRide] =
      taxiFactsDF
        .as[TaxiRide]

    val actualDistribution = processTaxiData(taxiFactsDS);

    actualDistribution.foreach { r =>
      val min = r.get(2).asInstanceOf[Double]
      val avg = r.get(3).asInstanceOf[Double]
      val max = r.get(4).asInstanceOf[Double]

      (min <= avg) shouldBe true
      (avg <= max) shouldBe true
      println(s"test OK")
    }

    val actualDistributionLimited = actualDistribution.limit(10)
    checkAnswer(
      actualDistributionLimited,
      Row("19",22030,0.01,2.41,46.3)  ::
        Row("20",21516,0.01,2.57,41.8) ::
        Row("22",20793,0.01,3.1,46.51) ::
        Row("21",20224,0.01,2.88,40.7) ::
        Row("23",19427,0.01,3.14,42.8) ::
        Row("09",18792,0.01,2.26,45.98) ::
        Row("18",18557,0.01,2.55,37.1) ::
        Row("16",17728,0.01,2.68,51.6) ::
        Row("15",17385,0.01,2.84,40.32) ::
        Row("10",16760,0.01,2.35,53.5) :: Nil
    )
  }

  test("part1 - getMostPopularBoroughs") {
    val taxiZonesDF2 = readCSV("src/main/resources/taxi_zones.csv")
    val taxiDF2 = readParquet("src/main/resources/yellow_taxi_jan_25_2018")

    val actualDistribution = getMostPopularBoroughs(taxiDF2, taxiZonesDF2)

    checkAnswer(
      actualDistribution,
      Row("Manhattan",296527)  ::
        Row("Queens",13819) ::
        Row("Brooklyn",12672) ::
        Row("Unknown",6714) ::
        Row("Bronx",1589) ::
        Row("EWR",508) ::
        Row("Staten Island",64) :: Nil
    )
  }
}

