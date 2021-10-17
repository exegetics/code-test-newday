# movies_ratings
Recruitment process in NewDay for a Data Engineer by Ben Stapley

### Building
```
sbt package
```

### Running/Submitting
```
spark-submit --master local[*] --class uk.co.newday.MoviesRatingsCandidate  ./target/scala-2.11/movies_ratings_2.11-1.0.jar <PATH_TO_MOVIE_DATA> <PATH_TO_RATINGS_DATA>
```
### Result

Four data sets in Parquet in the ```output``` directory ...
- movieRatings
- movies
- ratings
- ratingWithRankTop3