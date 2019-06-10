'''
CMSC 12300 Spring 19

Project: Item-based and User-based Movie Recommendation using MapReduce and Hadoop

Algorithm 1: Item-based Recommendation

Team Name: 0errors0warnings
Team Members: Boyang Qu, Ruixi Li, Tianxin Zheng, Haowen Shang
Instructior: Professor Matthew Wachs
'''

from math import sqrt
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
                    reducer=self.reducer_output_similarities)]


    def mapper_input(self, key, line):
        '''
        Convert the raw data to userID, (movie, rating).
        '''
        user_id, item_id, rating = line.split(',')
        yield  user_id, (item_id, float(rating))


    def reducer_ratings_by_user(self, user_id, item_ratings):
        '''
        Group (item, rating) pairs by userID.
        '''
        ratings = []
        for movieID, rating in item_ratings:
            ratings.append((movieID, rating))
        yield user_id, ratings


    def convert_itemRatings(self, final):
        '''
        Convert (item, rating) to (item1,item2) ,(rating1, rating2).
        '''
        l = len(final)
        res = []
        for i, pair in enumerate(final):
            j = 0
            item1, rating1 = pair[0], pair[1]
            while j < l:
                if j != i:
                    item2, rating2 = final[j]    
                    a = (item1, item2)
                    b = (rating1, rating2)
                    res.append((a, b))
                j = j + 1
        return res


    def mapper_create_item_pairs(self, user_id, item_ratings):
        '''
        Get pairwise items and pairwise ratings.
        '''
        final = []
        for item_id, rating in item_ratings:
            final.append((item_id, rating))        
        res = self.convert_itemRatings(final)
        for movie_pair, rating_pair in res:
            yield (movie_pair[0], movie_pair[1]),\
                    (rating_pair[0], rating_pair[1])
    

    def cosine_similarity(self, rating_pairs):
        '''
        Calculate the cosine similarity metric between two
        rating vectors.
        '''
        num_pairs = 0
        sum_xx = sum_yy = sum_xy = 0
        for ratingX, ratingY in rating_pairs:
            sum_xx += ratingX * ratingX
            sum_yy += ratingY * ratingY
            sum_xy += ratingX * ratingY
            num_pairs += 1

        numerator = sum_xy
        denominator = sqrt(sum_xx) * sqrt(sum_yy)

        score = 0
        if (denominator):
            score = (numerator / (float(denominator)))

        return (score, num_pairs)


    def reducer_compute_similarity(self, movie_pair, rating_pairs):
        '''
        Calculate the similarity score between the ratings vectors
        for each movie pair viewed by multiple people.
        Output (movie1, movie2), (score, number of co-ratings)
        '''

        score, num_pairs = self.cosine_similarity(rating_pairs)

        if (num_pairs > 10 and score > 0.95):
            yield movie_pair, (score, num_pairs)


    def mapper_sort_similarities(self, movie_pair, scores):
        '''
        Shuffle the results.
        '''
        score, n = scores
        movie1, movie2 = movie_pair

        yield (movie1, score), (movie2, n)


    def reducer_output_similarities(self, movie_score, similar_n):
        '''
        Change the output format to: movie1, (movie2, score, num_pairs)
        '''
        movie1, score = movie_score
        for movie2, n in similar_n:
            yield movie1, (movie2, score, n)


if __name__ == '__main__':
    MoviesSimilarities.run()