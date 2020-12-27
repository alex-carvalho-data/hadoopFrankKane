ratings = LOAD '/user/maria_dev/ml-100k/u.data' 
    AS (userId:int, movieId:int, rating:int, ratingTime:int);

metadata = LOAD '/user/maria_dev/ml-100k/u.item'
    USING PigStorage('|')
    AS (movieId:int, movieTitle:chararray, 
        releaseDate:chararray, videoRelease:chararray, 
        imdb:chararray);

DUMP metadata;

nameLookup = FOREACH metadata GENERATE movieId, movieTitle,
    ToUnixTime(ToDate(releaseDate, 'dd-MMM-yyyy')) AS releaseTime;

ratingsByMovie = GROUP ratings BY movieId;

DUMP ratingsByMovie;

avgRatings = FOREACH ratingsByMovie GENERATE group 
    AS movieId, AVG(ratings.rating) AS avgRating;

DUMP avgRating; 

DESCRIBE ratings;
DESCRIBE ratingsByMovie;
DESCRIBE avgRatings;

fiveStarMovies = FILTER avgRatings BY avgRating > 4.0;

DESCRIBE fiveStarMovies;
DESCRIBE nameLookup;

fiveStarsWithData = JOIN fiveStarMovies BY movieId, nameLookup BY movieId;

DESCRIBE fiveStarsWithData;

DUMP fiveStarsWithData;

oldestFiveStarMovies = ORDER fiveStarsWithData BY nameLookup::releaseTime;

DUMP oldestFiveStarMovies;