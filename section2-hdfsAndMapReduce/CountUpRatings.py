from mrjob.job import MRJob
from mrjob.step import MRStep


class CountUpRatings(MRJob):
    def mapper_get_movie_ids(self, _, line):
        (userId, movieId, rating, timestamp) = line.split('\t')
        yield movieId, 1

    def reducer_count_movies(self, key, values):
        yield key, sum(values)

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_movie_ids,
                   reducer=self.reducer_count_movies)
        ]

if __name__ == '__main__':
    CountUpRatings.run()