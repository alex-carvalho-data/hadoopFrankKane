# It sorts all movies by its popularity (how many times it was rated)
from mrjob.job import MRJob
from mrjob.step import MRStep
import logging


class MovieRanker(MRJob):
    def mapper_get_movies(self, _, line):
        (userId, movieId, rating, timestamp) = line.split('\t')
        yield movieId, 1

    def reducer_count_movies(self, key, values):
        # Instructor suggestion:
        # Trick to sort numeric string properly (prefix with zeros)
        yield str(sum(values)).zfill(5), key

    def reducer_flip_key_value(self, movie_count, movie_ids):
        # Instructor suggestion:
        # Reducer returning more lines then the Mapper
        for movie_id in movie_ids:
            yield movie_id, movie_count

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_movies,
                   reducer=self.reducer_count_movies),
            MRStep(reducer=self.reducer_flip_key_value)
        ]


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

    MovieRanker.run()

# command to run it using hadoop engine on hdp server
