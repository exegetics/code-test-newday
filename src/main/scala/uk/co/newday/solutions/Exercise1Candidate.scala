package uk.co.newday.solutions

import org.apache.spark.sql.{DataFrame, Dataset, Encoder, SparkSession}

object Exercise1Candidate {

  private case class Movie(movieId: Int, title: String, genre: String)

  private case class Rating(userId: Int, movieId: Int, rating: Int, timestamp: Int)

  def execute(movieFilePath: String, ratingFilePath: String)(implicit spark: SparkSession): (DataFrame, DataFrame) = {
    //Please load movies and ratings csv's in output dataframes.
    import spark.implicits._
    (loadData[Movie](movieFilePath),
      loadData[Rating](ratingFilePath)
    )
  }

  private def loadData[T: Encoder](path: String)(implicit sparkSession: SparkSession) = {
    import sparkSession.implicits._

    //Read each row as a single string
    val rowAsString = sparkSession.read.text(path).as[String]

    // Replace the '::' delimiter with \t
    val rowAsStringTabDelimited: Dataset[String] = rowAsString.map {
      _.replace("::", "\t")
    }

    //Convert string to dataframe with the required  schema
    sparkSession.read.option("sep", "\t").schema(implicitly[Encoder[T]].schema).csv(rowAsStringTabDelimited)
  }


}
