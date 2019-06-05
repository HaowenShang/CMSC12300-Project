'''
CMSC 12300 Spring 19

Project: Item-based and User-based Movie Recommendation using MapReduce and Hadoop

Algorithm 1: Item-based Recommendation

Team Name: 0errors0warnings
Team Members: Boyang Qu, Ruixi Li, Tianxin Zheng, Haowen Shang
Instructior: Professor Matthew Wachs
'''

import numpy as np
from math import sqrt
from scipy.sparse import csr_matrix
from collections import Counter
from mrjob.job import MRJob
from mrjob.step import MRStep


class MoviesSimilarities(MRJob):
 
    def steps(self):
        return [
            MRStep(mapper=self.mapper_input,
                    reducer=self.reducer_ratings_by_user),
            MRStep(mapper=self.mapper_create_item_pairs,
                    reducer=self.reducer_compute_similarity),
            MRStep(mapper=self.mapper_sort_similarities,
                    mapper_init=self.load_movie_names,
                    reducer=self.reducer_output_similarities)]

    def mapper_input(self, key, line): # groupe by user rating

        user_id, item_id, rating = line.split(',')
        yield  user_id, (item_id, float(rating))

    def reducer_ratings_by_user(self, user_id, itemRatings):
        #Group (item, rating) pairs by userID

        ratings = []
        for movieID, rating in itemRatings:
            ratings.append((movieID, rating))
        yield user_id, ratings


    def expand_ratings(self, final):
        c = len(final)
        tot = []
        for i, pair in enumerate(final):
            j = i + 1
            cur_pair = pair
            while j < c:
                nextpair = final[j]    
                a = (cur_pair[0], nextpair[0])
                b = (cur_pair[1], nextpair[1])
                tot.append((a, b))
                j = j + 1
        return tot

    def mapper_create_item_pairs(self, user_id, itemRatings): # get pairwise items
        final = []
        for item_id, rating in itemRatings:
            final.append((item_id, rating))        
        rat = self.expand_ratings(final)
        for movie_pair, rating_pair in rat:
            yield (movie_pair[0], movie_pair[1]),\
                    (rating_pair[0], rating_pair[1])
    
    def cosine_similarity(self, ratingPairs):
        # Computes the cosine similarity metric between two
        # rating vectors.
        numPairs = 0
        sum_xx = sum_yy = sum_xy = 0
        for ratingX, ratingY in ratingPairs:
            sum_xx += ratingX * ratingX
            sum_yy += ratingY * ratingY
            sum_xy += ratingX * ratingY
            numPairs += 1

        numerator = sum_xy
        denominator = sqrt(sum_xx) * sqrt(sum_yy)

        score = 0
        if (denominator):
            score = (numerator / (float(denominator)))

        return (score, numPairs)

    def reducer_compute_similarity(self, moviePair, ratingPairs):
        # Compute the similarity score between the ratings vectors
        # for each movie pair viewed by multiple people

        # Output movie pair => score, number of co-ratings

        score, numPairs = self.cosine_similarity(ratingPairs)

        # Enforce a minimum score and minimum number of co-ratings
        # to ensure quality
        if (numPairs > 5 and score > 0.95):
            yield moviePair, (score, numPairs)
    
    def configure_options(self):
        super(MoviesSimilarities, self).configure_options()
        self.add_file_arg('--items')

    def load_movie_names(self):
        # Load database of movie names to print pretty name instead ID.
        self.movieNames = {}

        with open(self.options.items) as f:
            for line in f:
                fields = line.split(',')
                if fields[0] != 'movieId':
                    self.movieNames[int(fields[0])] = fields[1]

    def mapper_sort_similarities(self, moviePair, scores):
        # Shuffle things around so the key is (movie1, score)
        # so we have meaningfully sorted results.
        score, n = scores
        movie1, movie2 = moviePair

        yield (self.movieNames[int(movie1)], score), \
            (self.movieNames[int(movie2)], n)

    def reducer_output_similarities(self, movieScore, similarN):
        # Output the results.
        # Movie => Similar Movie, score, number of co-ratings
        movie1, score = movieScore
        for movie2, n in similarN:
            yield movie1, (movie2, score, n)



if __name__ == '__main__':
    MoviesSimilarities.run()