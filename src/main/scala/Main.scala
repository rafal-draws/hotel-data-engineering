import UDFs.MakeIndexUDF
import analyzers.HotelReviewsAnalyzer
import cleaners.DataCleaner
import loaders.DataLoader
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataTypes

object Main {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder().master("local[*]")
      .appName("week-one")
      .getOrCreate()

    val dataLoader: DataLoader = new DataLoader(spark)
    val dataCleaner: DataCleaner = new DataCleaner(spark)
    val hotelReviewsAnalyzer: HotelReviewsAnalyzer = new HotelReviewsAnalyzer(spark)


    val makeIndexUDF: MakeIndexUDF = new MakeIndexUDF()
    spark.udf.register("makeIndexUDF", makeIndexUDF, DataTypes.StringType)



    val loadedDF = dataLoader.loadAll().cache()
    val cleanedDF = dataCleaner.cleanHotelReviews(loadedDF)


    val hotelsRangingFrom6to7inReviewerScore: Dataset[Row] = hotelReviewsAnalyzer.bestHotelsInScoreRange(cleanedDF, 6,7)

    val MostUsedWordsUnitedKingdom: Dataset[Row] = hotelReviewsAnalyzer.mostUsedInterestingWordsPerCountry(cleanedDF, "United")
    val MostUsedWordsUS: Dataset[Row] = hotelReviewsAnalyzer.mostUsedInterestingWordsPerCountry(cleanedDF, "US")

    //TODO    val worstOrBestHotels: Dataset[Row] = hotelReviewsAnalyzer.showWorstHotelPerCountry(cleanedDF, worst = true)

    val indexedDFforOptimalization: Dataset[Row] = cleanedDF.withColumn("Index",
      call_udf("makeIndexUDF",
        col("Review_Date"), col("Hotel_Name"), col("Hotel_Rating")))

    cleanedDF.write.mode(SaveMode.Overwrite).parquet("/output/HotelData")
  }


}
