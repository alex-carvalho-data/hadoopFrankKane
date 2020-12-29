from pyspark.sql import Row
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from pyspark.ml.recommendation import ALS


# Load up movie Id -> movie name dictionary
def load_movie_names():
    movie_names = {}
    with open('/home/maria_dev/ml-100k/u.item') as f:
        for line in f:
            fields = line.split('|')
            movie_names[int(fields[0])] = fields[1].decode('ascii', 'ignore')
    return movie_names


# Convert u.data lines into (user_id, movie_id, rating) rows
def parse_input(line):
    fields = line.value.split()
    return Row(user_id=int(fields[0]),
               movie_id=int(fields[1]),
               rating=float(fields[2]))


if __name__ == '__main__':
    # Create a SparkSession
    spark = SparkSession.builder.appName('MovieRecommendations').getOrCreate()

    # Load up movie Id -> name dictionary
    movieNames = load_movie_names()

    # Get the raw data
    linesRDD = spark.read.text('hdfs:///user/maria_dev/ml-100k/u.data').rdd

    # Convert it to a RDD of Row objects with (user_id, movie_id, rating)
    ratingsRDD = linesRDD.map(parse_input)

    # Convert to a DataFrame and cache it
    ratingsDF = spark.createDataFrame(ratingsRDD).cache()

    # Create an ALS collaborative filtering model from the complete data set
    als = ALS(maxIter=5,
              regParam=0.01,
              userCol='user_id',
              itemCol='movie_id',
              ratingCol='rating')
    model = als.fit(ratingsDF)

    # Print out ratings from user 0:
    print('\nRatings for user Id 0:')
    userRatings = ratingsDF.filter('user_id = 0')
    for rating in userRatings.collect():
        print(movieNames[rating['movie_id']], rating['rating'])

    print('\nTop 20 recommendations:')
    # Find movies rated more than 100 times
    ratingCountsDF = \
        ratingsDF.groupBy('movie_id').count().filter('count > 100')
    # Construct a 'test' DataFrame for user 0 with every movie rated more than
    # 100 times
    popularMoviesDF = \
        ratingCountsDF.select('movie_id').withColumn('user_id', lit(0))

    # Run the model on the list of popular movies for user Id 0
    recommendationsDF = model.transform(popularMoviesDF)

    print('recommendationsDF')
    recommendationsDF.printSchema()

    # Get the top 20 movies with the highest predicted rating for user 0
    topRecommendationsDF = \
        recommendationsDF.sort(recommendationsDF.prediction.desc()).take(20)

    for recommendation in topRecommendationsDF:
        print(movieNames[recommendation['movie_id']],
              recommendation['prediction'])

# To run it in hdp sand box:
# set spark 2 environment variable
# [xx]$ export SPARK_MAJOR_VERSION=2
# invoke spark-submit script
# [xx]$ spark-submit LowestRatedMovieDF.py
