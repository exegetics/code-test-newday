package uk.co.newday.solutions

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{max, _}

object Exercise2Candidate {

  private val joinColumns = Seq("movieId")
  private val grouping = Seq("movieId", "title", "genre").map {
    col(_)
  }

  def execute(movies: DataFrame, ratings: DataFrame): DataFrame = {
    //With two dataframe apply the join as specified in the exercise.
    movies.join(ratings, joinColumns, "left").groupBy(grouping: _*).agg(
      max("rating").as("maxRating"),
      min("rating").as("minRating"),
      avg("rating").as("avgRating")
    )
  }
}
