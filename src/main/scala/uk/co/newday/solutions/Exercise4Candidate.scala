package uk.co.newday.solutions

import org.apache.spark.sql.DataFrame

object Exercise4Candidate {


  def execute(movies: DataFrame, ratings: DataFrame, movieRatings: DataFrame, ratingWithRankTop3: DataFrame): Unit = {
    //write all the output in parquet format
    write("./output/movies", movies)
    write("./output/ratings", ratings)
    write("./output/movieRatings", movieRatings)
    write("./output/ratingWithRankTop3", ratingWithRankTop3)
  }

  private def write(path: String, df: DataFrame): Unit = {
    df.write.save(path)
  }

}
