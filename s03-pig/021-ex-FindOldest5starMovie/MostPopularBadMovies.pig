ratings = LOAD '/user/maria_dev/ml-100k/u.data'
    AS (userId:int, movieId:int, rating:int);

ratingsByMovie = GROUP ratings BY movieId;

movieRatingsAvgCount = FOREACH ratingsByMovie
    GENERATE group 
    AS movieId, 
       AVG(ratings.rating) AS avgRating,
       COUNT(ratings.movieId) AS movieCount;

oneStarMovies = FILTER movieRatingsAvgCount BY avgRating < 2.0;

metadata = LOAD '/user/maria_dev/ml-100k/u.item'
    USING PigStorage('|')
    AS (movieId:int, movieTitle:chararray);

oneStarMoviesInfo = JOIN oneStarMovies BY movieId,
                    metadata BY movieId;

oneStarMoviesInfoPopularSort = ORDER oneStarMoviesInfo BY oneStarMovies::movieCount DESC;

DUMP oneStarMoviesInfoPopularSort;