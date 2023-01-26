import loaders.DataLoader
import org.apache.spark.sql.SparkSession

object Main {

  def main(args: Array[String]) = {
    val spark: SparkSession = SparkSession
      .builder().master("local[*]")
      .appName("week-one")
      .getOrCreate()

    val dataLoader: DataLoader = new DataLoader(spark)


    val DF = dataLoader.loadAll().cache()
  }


}
