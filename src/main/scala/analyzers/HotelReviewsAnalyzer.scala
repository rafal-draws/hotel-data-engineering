package analyzers

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, Row, SparkSession}

class HotelReviewsAnalyzer(sparkSession: SparkSession) {

  def bestHotelsInScoreRange(df: Dataset[Row], start: Double, stop:Double): Dataset[Row] ={
    df.groupBy(col("Hotel_Name")).agg(avg("Reviewer_Score").as("Avg_Score")).sort(desc("Avg_Score"))
      .filter(col("Avg_Score").geq(start) && col("Avg_Score").leq(stop))

  }


}
