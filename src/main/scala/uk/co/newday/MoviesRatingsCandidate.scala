package uk.co.newday

import org.apache.spark.sql.SparkSession
import uk.co.newday.solutions.{Exercise1Candidate, Exercise2Candidate, Exercise3Candidate, Exercise4Candidate}


object MoviesRatingsCandidate {
  lazy private implicit val spark = SparkSession.builder().master("local[*]").appName("NewDay coding exerices").getOrCreate()

  def main(args: Array[String]) {
    val (movieFilePath, ratingsFilePath) = args.take(2) match {
      case Array(a,b) => (a,b)
      case _ => println("Command usage: <PATH_TO_MOVIE_DATA> <PATH_TO_RATING_DATA>")
        sys.exit()
    }
    spark.sparkContext.setLogLevel("warn")
    val (movies, ratings) = Exercise1Candidate.execute(movieFilePath, ratingsFilePath)
    movies.show()
    ratings.show()
    val movieRatings = Exercise2Candidate.execute(movies, ratings)
    movieRatings.show()
    val ratingWithRankTop3 = Exercise3Candidate.execute(movies, ratings)
    ratingWithRankTop3.show(1000)
    Exercise4Candidate.execute(movies, ratings, movieRatings, ratingWithRankTop3)
  }
}
