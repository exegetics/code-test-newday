package uk.co.newday.solutions

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object Exercise3Candidate {

  private val windowUserRatings = Window.partitionBy("userId").orderBy(col("rating").desc)

  def execute(movies: DataFrame, ratings: DataFrame): (DataFrame) = {

    movies.join(ratings, Seq("movieId"), "left").
      withColumn("rank", rank().over(windowUserRatings)).
      filter(col("rank") <= lit(3))

  }
}
