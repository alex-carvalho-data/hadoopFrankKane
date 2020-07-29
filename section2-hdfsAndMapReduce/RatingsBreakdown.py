from mrjob.job import MRJob
from mrjob.step import MRStep


class RatingsBreakdown(MRJob):
    # I couldn't see the necessity of a Generator. No state will be
    # hold.
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_ratings,
                   reducer=self.reducer_count_rating)
        ]

    def mapper_get_ratings(self, _, line):
        (userID, movieID, rating, timestamp) = line.split('\t')
        yield rating, 1

    # Once more, I couldn't see the necessity of a Generator
    def reducer_count_rating(self, key, values):

        yield key, sum(values)


if __name__ == '__main__':
    RatingsBreakdown.run()
