from mrjob.job import MRJob
from mrjob.step import MRStep


class MovieRatings(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_movie_id,
                   reducer=self.reducer_movie_id_count),
            MRStep(reducer=self.reducer_switch_ordererd)
        ]

    def mapper_movie_id(self, _, line):
        (user_id, movie_id, rating, timestamp) = line.split('\t')
        yield movie_id, 1

    def reducer_movie_id_count(self, movie_id, ones):
        yield str(sum(ones)).zfill(6), movie_id

    def reducer_switch_ordererd(self, count, movie_ids):
        for movie_id in movie_ids:
            yield movie_id, count


if __name__ == '__main__':
    MovieRatings.run()


# Run it at hadoop cluster
# python MovieRatings.py -r hadoop --hadoop-streaming-jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar ~/u.data