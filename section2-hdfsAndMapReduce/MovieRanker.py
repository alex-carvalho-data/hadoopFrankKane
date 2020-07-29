# It sorts all movies by its popularity (how many times it was rated)
from mrjob.job import MRJob
from mrjob.step import MRStep


class MovieRanker(MRJob):
    def mapper_get_movies(self, _, line):
        (userId, movieId, rating, timestamp) = line.split('\t')
        yield movieId, 1

    def reducer_count_movies(self, key, values):
        yield sum(values), key

    def reducer_flip_key_value(self, movie_Count, movie_ids):
        movie_ids_str = ''
        for movie_id in movie_ids:
            movie_ids_str += movie_id + ' '
        yield movie_ids_str, movie_Count

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_movies,
                   reducer=self.reducer_count_movies),
            MRStep(reducer=self.reducer_flip_key_value)
        ]


if __name__ == '__main__':
    MovieRanker.run()
