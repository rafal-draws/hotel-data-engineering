package cleaners

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.{Dataset, Row, SparkSession}

class DataCleaner (sparkSession: SparkSession){

  def cleanHotelReviews(df: Dataset[Row]): Dataset[Row] ={


    val cleanedDF: Dataset[Row] =
      df
      .withColumn("Reviewer_Score",
              col("Reviewer_Score").cast(DataTypes.DoubleType))
      .na.drop(Seq("Reviewer_Score"))
      .na.fill("not disclosed")
        .withColumn("Review_Date", col("Review_Date").cast(DataTypes.DateType))

  cleanedDF
  }


}
