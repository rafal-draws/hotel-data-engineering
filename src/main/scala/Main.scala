import analyzers.HotelReviewsAnalyzer
import cleaners.DataCleaner
import loaders.DataLoader
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object Main {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder().master("local[*]")
      .appName("week-one")
      .getOrCreate()

    val dataLoader: DataLoader = new DataLoader(spark)
    val dataCleaner: DataCleaner = new DataCleaner(spark)
    val hotelReviewsAnalyzer: HotelReviewsAnalyzer = new HotelReviewsAnalyzer(spark)


    val loadedDF = dataLoader.loadAll().cache()
    val cleanedDF = dataCleaner.cleanHotelReviews(loadedDF)


    val hotelsRangingFrom6to7inReviewerScore: Dataset[Row] = hotelReviewsAnalyzer.bestHotelsInScoreRange(cleanedDF, 6,7)

    val MostUsedWordsUnitedKingdom: Dataset[Row] = hotelReviewsAnalyzer.mostUsedInterestingWordsPerCountry(cleanedDF, "United")
    val MostUsedWordsUS: Dataset[Row] = hotelReviewsAnalyzer.mostUsedInterestingWordsPerCountry(cleanedDF, "US")

    //    val worstOrBestHotels: Dataset[Row] = hotelReviewsAnalyzer.showWorstHotelPerCountry(cleanedDF, worst = true)


    cleanedDF.write.mode(SaveMode.Overwrite).parquet("/output/HotelData")
  }


}
