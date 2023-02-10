package UDFs

import org.apache.spark.sql.api.java.UDF3

import scala.util.Random

class MakeIndexUDF extends UDF3[String, String, Double, String]{

  def randomAlphanumericString(length: Int): String = {
    Iterator.continually(Random.nextPrintableChar)
      .filter(_.isLetter)
      .take(length)
      .mkString

  }
  override def call(date: String, name: String, rating: Double): String = {
    s"$date$name$rating${randomAlphanumericString(5)}"
  }

}
