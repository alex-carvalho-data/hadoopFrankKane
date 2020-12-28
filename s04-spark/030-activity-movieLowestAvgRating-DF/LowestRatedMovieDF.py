from pyspark.sql import Row
from pyspark.sql import SparkSession


def load_movie_names():
    movie_names = {}
    with open('/home/maria_dev/ml-100k/u.item') as f:
        for line in f:
            fields = line.split('|')
            movie_names[fields[0]] = fields[1]

    return movie_names


def parse_input(line):
    fields = line.split()
    return Row(movie_id=int(fields[1]), rating=float(fields[2]))


if __name__ == '__main__':
    # Create a SparkSession
    spark = SparkSession.builder.appName('PopularMovies').getOrCreate()

    # Load up movie_id -> name dictionary
    movieNames = load_movie_names()

    # Get the raw data
    linesRDD = \
        spark.sparkContext.textFile('hdfs:///user/maria_dev/ml-100k/u.data')
    # Convert it to a RDD of Row objects with (movie_id, rating)
    moviesRDD = linesRDD.map(parse_input)
    # Convert that to a DataFrame
    moviesDataFrame = spark.createDataFrame(moviesRDD)

    # Compute average rating for reach movie_id
    averageRatingsDataFrame = moviesDataFrame.groupBy('movie_id').avg('rating')

    # Compute count of ratings for each movie_id
    countsDataFrame = moviesDataFrame.groupby('movie_id').count()

    # Join the two together
    # (We now have movie_id, avg(rating) and count columns)
    averageAndCountsDF = \
        averageRatingsDataFrame.join(countsDataFrame, 'movie_id')

    # Pull the top 10 results
    topTenDataFrame = averageAndCountsDF.orderBy('avg(rating)').take(10)

    # Print them out, converting movie_id's to names as we go
    for movie in topTenDataFrame:
        print(movieNames[movie[0]], movie[1], movie[2])

    # Stop the session
    spark.stop()
