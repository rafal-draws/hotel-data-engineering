package analyzers

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, Row, SparkSession}

class HotelReviewsAnalyzer(sparkSession: SparkSession) {

  def bestHotelsInScoreRange(df: Dataset[Row], start: Double, stop:Double): Dataset[Row] ={
    df.groupBy(col("Hotel_Name")).agg(avg("Reviewer_Score").as("Avg_Score")).sort(desc("Avg_Score"))
      .filter(col("Avg_Score").geq(start) && col("Avg_Score").leq(stop))

  }

  def mostUsedInterestingWordsPerCountry(df: Dataset[Row], country: String): Dataset[Row] ={ //TODO
    df.filter(col("Country").contains(lit(country)))
      .withColumn("Review", regexp_replace(col("Review"), "Positive:", ""))
      .withColumn("Review", regexp_replace(col("Review"), "Negative:", ""))
      .withColumn("Review", regexp_replace(col("Review"), "^\\ ", ""))
      .withColumn("Review", regexp_replace(col("Review"), "\\.", ""))
      .withColumn("Split", split(col("Review"), " "))
      .withColumn("Split", explode(col("Split")))
      .filter(length(col("Split")).geq(8))
      .groupBy("Split").count().orderBy(desc("count"))
  }

  def mostUsedWordsInOrderPerRatingPerHotel(df: Dataset[Row]):Dataset[Row] ={
    df
    .withColumn("Review", regexp_replace(col("Review"), "Positive:", ""))
    .withColumn("Review", regexp_replace(col("Review"), "Negative:", ""))
    .withColumn("Review", regexp_replace(col("Review"), "^\\ ", ""))
    .withColumn("Review", regexp_replace(col("Review"), "\\.", ""))
    .withColumn("Split", split(col("Review"), " "))
    .withColumn("Split", explode(col("Split")))
      .groupBy("Hotel_Name", "Reviewer_Score")
      .agg(collect_list("Split")) //TODO map occurances

  }
}
