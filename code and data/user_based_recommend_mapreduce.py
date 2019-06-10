'''
CMSC 12300 Spring 19

Project: Item-based and User-based Movie Recommendation using MapReduce and Hadoop

Algorithm 2: User-based Recommendation

Team Name: 0errors0warnings
Team Members: Boyang Qu, Ruixi Li, Tianxin Zheng, Haowen Shang
Instructior: Professor Matthew Wachs

Execute the file:
   $ python3 user_based_recommend_mapreduce.py < training_ratings.csv > user_10nearestneighbor.csv
'''

from math import sqrt
from mrjob.job import MRJob
from mrjob.step import MRStep

class UsersSimilarities(MRJob): 
    def steps(self):
        return [
            MRStep(mapper=self.mapper_input,
                    reducer=self.reducer_ratings_by_user),
            MRStep(mapper=self.mapper_create_item_pairs,
                    reducer=self.reducer_compute_similarity),
            MRStep(mapper=self.mapper_sort_similarities,
                    reducer=self.reducer_output_similarities),
            MRStep(mapper=self.mapper_knn,
                    reducer=self.reducer_knn)]

    def mapper_input(self, key, line):
        '''
        Convert the raw data to item_id, (user_id, float(rating)).
        '''
        user_id, item_id, rating = line.split(',')
        yield  item_id, (user_id, float(rating))

    def reducer_ratings_by_user(self, item_id, itemRatings):
        '''
        Group (user_id, rating) pairs by userID
        '''
        ratings = []
        for user_id, rating in itemRatings:
            ratings.append((user_id, rating))
        yield item_id, ratings


    def convert_itemRatings(self, final):
        c = len(final)
        res = []
        for i, pair in enumerate(final):
            j = i + 1
            item1, rating1 = pair[0], pair[1]
            while j < c:
                item2, rating2 = final[j]    
                a = (item1, item2)
                b = (rating1, rating2)
                res.append((a, b))
                j = j + 1
        return res

    def mapper_create_item_pairs(self, item_id, itemRatings): 
        '''
        Get pairwise items.
        '''
        final = []
        for user_id, rating in itemRatings:
            final.append((user_id, rating))        
        res = self.convert_itemRatings(final)
        for user_pair, rating_pair in res:
            yield (user_pair[0], user_pair[1]),\
                    (rating_pair[0], rating_pair[1])
    
    def cosine_similarity(self, ratingPairs):
        '''
        Computes the cosine similarity metric between two
        rating vectors.
        '''
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

    ### pearson_correlation start ###
    def pearson_correlation(self, user1, user2):
        result = 0.0
        user1_data = self.uu_dataset[user1]
        user2_data = self.uu_dataset[user2]

        rx_avg = self.user_average_rating(user1_data)
        ry_avg = self.user_average_rating(user2_data)
        sxy = self.common_items(user1_data, user2_data)

        top_result = 0.0
        bottom_left_result = 0.0
        bottom_right_result = 0.0
        for item in sxy:
            rxs = user1_data[item]
            rys = user2_data[item]
            top_result += (rxs - rx_avg)*(rys - ry_avg)
            bottom_left_result += pow((rxs - rx_avg), 2)
            bottom_right_result += pow((rys - ry_avg), 2)
        bottom_left_result = math.sqrt(bottom_left_result)
        bottom_right_result = math.sqrt(bottom_right_result)

        denominator = bottom_left_result * bottom_right_result

        if denominator:
            return top_result / denominator
        else:
            return 0.0

    def user_average_rating(self, user_data):
        avg_rating = 0.0
        size = len(user_data)
        for (movie, rating) in user_data.items():
            avg_rating += float(rating)
        avg_rating /= size * 1.0
        return avg_rating

    def common_items(self, user1_data, user2_data):
        result = []
        ht = {}
        for (movie, rating) in user1_data.items():
            ht.setdefault(movie, 0)
            ht[movie] += 1
        for (movie, rating) in user2_data.items():
            ht.setdefault(movie, 0)
            ht[movie] += 1
        for (k, v) in ht.items():
            if v == 2:
                result.append(k)
        return result
    ### pearson_correlation end ###

    def reducer_compute_similarity(self, userPair, ratingPairs):
        '''
        Compute the similarity score between the ratings vectors
        for each user pair of same movie
        '''
        # Output user pair => score, number of co-ratings
        score, numPairs = self.cosine_similarity(ratingPairs)
        # Enforce a minimum score and minimum number of co-ratings
        # to ensure quality
        yield userPair, (score, numPairs)

    def mapper_sort_similarities(self, userPair, scores):
        '''
        Shuffle things around so the key is (user1, score)
        so we have meaningfully sorted results.
        '''
        score, n = scores
        user1, user2 = userPair

        yield (user1, score), (user2, n)

    def reducer_output_similarities(self, movieScore, similarN):
        '''
        User => Similar User, score, number of co-ratings
        '''
        user1, score = movieScore
        for user2, n in similarN:
            yield user1, (user2, score)

    def mapper_knn(self, user1, user_score):
        '''
        Map same user and their similar users.
        '''
        if user1 and user_score:
            yield user1, user_score

    def reducer_knn(self, user1, user_score):
        '''
        Output the result: user and its 10 nearest neighbors.
        '''
        users_scores = list(user_score)
        result = []
        sorted_users_scores = sorted(users_scores, 
            key=lambda x: (x[1], x[0]), reverse=True) 
        for i in range(10):
            if i >= len(sorted_users_scores):
                break
            result.append(sorted_users_scores[i])
        yield user1, result

if __name__ == '__main__':
    UsersSimilarities.run()

