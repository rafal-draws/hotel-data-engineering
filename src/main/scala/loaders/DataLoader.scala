package loaders

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.{Dataset, Row, SparkSession}

class DataLoader (sparkSession: SparkSession){

  def loadAll(): Dataset[Row] = {
    val netherlandsBig: Dataset[Row] = loadNetherlandsBiggerReviews()
    val netherlandsSmall: Dataset[Row] = loadAmsterdamLesserDataset()
    val datafiniti: Dataset[Row] = loadDatafinitiHotelReviews()
    val usData: Dataset[Row] = loadMassiveUSdata()


    val joinedDF = netherlandsBig
      .unionByName(datafiniti, allowMissingColumns = true)
      .unionByName(netherlandsSmall, allowMissingColumns = true)
      .unionByName(usData, allowMissingColumns = true)



  joinedDF
  }

  def loadNetherlandsBiggerReviews(): Dataset[Row] = {
  val initDF: Dataset[Row] = sparkSession.read.option("header", "true").csv("Hotel_Reviews.csv")
  val countryDefinedDF: Dataset[Row] = initDF.withColumn("Country", split(col("Hotel_Address")," "))
      .withColumn("Country", col("Country")(size(col("Country"))-1))


  val transformedDF: Dataset[Row] = countryDefinedDF
    .select("Review_Date", "Hotel_Name", "Reviewer_Score", "Negative_Review", "Positive_Review", "Country", "Hotel_Address")
    .withColumn("Review", concat(lit("Positive:"), col("Positive_Review"), lit(" Negative:"), col("Negative_Review")))

    .drop("Negative_Review", "Positive_Review")
    .withColumn("Review_Date", regexp_replace(col("Review_Date"), "/"," "))
    .withColumn("Review_Date", split(col("Review_Date"), " "))
    .withColumn("Review_Date",
      concat
          (col("Review_Date")(2),lit("-"),
            col("Review_Date")(0),lit("-"),
            col("Review_Date")(1)
          )
    .cast(DataTypes.DateType))




    transformedDF

  }

  def loadMassiveUSdata(): Dataset[Row] ={
    val initDF: Dataset[Row] = sparkSession.read.option("header", "true").csv("7282_1.csv")

    val transformedDF: Dataset[Row] = initDF.select(
      col("`reviews.date`").as("Review_Date"),
      col("name").as("Hotel_Name"),
      col("`reviews.rating`").as("Reviewer_Score"),
      col("`reviews.text`").as("Review"),
      col("Country"),
      col("Address").as("Hotel_Address"))
      .withColumn("Review_Date", split(col("Review_Date"), "T")(0)
        .cast(DataTypes.DateType))



    transformedDF
  }

  def loadDatafinitiHotelReviews(): Dataset[Row] = {
    val initDF: Dataset[Row] = sparkSession.read.option("header", "true").csv("Datafiniti_Hotel_Reviews.csv")

    val transformedDF: Dataset[Row] = initDF
      .select(col("dateAdded").as("Review_Date"),
      col("name").as("Hotel_Name"),
      col("`reviews.rating`").as("Reviewer_Score"),
      col("`reviews.text`").as("Review"),
      col("country").as("Country"),
      col("address").as("Hotel_Address"))

    val adjustedScoreCastedDF: Dataset[Row] = transformedDF
      .withColumn("Reviewer_Score", expr("Reviewer_Score * 2"))
      .withColumn("Review_Date", split(col("Review_Date"), "T")(0).cast(DataTypes.DateType))

    adjustedScoreCastedDF
  }

  def loadAmsterdamLesserDataset(): Dataset[Row] = {
    val initDF: Dataset[Row] = sparkSession.read.option("header", "true").csv("HotelFinalDataset.csv")

    val transformed: Dataset[Row] = initDF
      .drop("_c0", "Unnamed: 0")
      .withColumn("Country", lit("Netherlands"))


    val finalDF: Dataset[Row] = transformed
      .select(
        col("Place").as("Hotel_Address"),
        col("Name").as("Hotel_Name"),
        col("Rating").as("Reviewer_Score"),
        col("Country").as("Country")
      )

    finalDF
  }

}
