from pyspark import SparkConf, SparkContext
# Run it in cluster with spark-submit
# spark-submit LowestRatedMovies.py


# This function just creates a Python "dictionary" we can later use to convert
# movie Id's to movie names while printing out the final results.
def load_movie_names():
    movie_names = {}
    with open('/home/maria_dev/ml-100k/u.item') as f:
        for line in f:
            fields = line.split('|')
            movie_names[int(fields[0])] = fields[1]

    return movie_names


# Take each line of u.data and convert it to (movieId, (rating, 1.0))
# This way we can then add up all the ratings for each movie, and the total of
# ratings for each movie (which lets us compute the average)
def parse_input(line):
    fields = line.split()

    return int(fields[1]), (float(fields[2]), 1.0)


if __name__ == '__main__':
    # The main script - create ou SparkContext
    conf = SparkConf().setAppName('WorstMovies')
    sc = SparkContext(conf=conf)

    # Load up out movie Id -> movie name lookup table
    movieNames = load_movie_names()

    # Load up the raw u.data file
    linesRdd = sc.textFile('hdfs:///user/maria_dev/ml-100k/u.data')

    # Convert to (movieId, (rating, 1.0))
    movieRatingsRdd = linesRdd.map(parse_input)

    # Reduce to (movieId, (sumOfRatings, totalRatings))
    ratingTotalAndCount = \
        movieRatingsRdd.reduceByKey(lambda accumulated_movie, current_movie:
                                    (accumulated_movie[0] + current_movie[0],
                                     accumulated_movie[1] + current_movie[1]))

    # Map to (rating, averageRating)
    averageRatings = \
        ratingTotalAndCount.mapValues(lambda total_and_count:
                                      total_and_count[0] / total_and_count[1])

    # Sort by average rating
    sortedMovies = averageRatings.sortBy(lambda x: x[1])

    # Take the top 10 results
    results = sortedMovies.take(10)

    # Print them out
    for result in results:
        print(movieNames[result[0]], result[1])
