from mrjob.job import MRJob
from mrjob.step import MRStep
from math import sqrt

from calc_similarities import norm,cosine,jaccard,generalized_jaccard,pearson_correlation,normalized_pearson_correlation

def expand_ratings(l1):
    c = len(l1)
    i = 0;
    tot = []
    for idx, elem in enumerate(l1):
        i = i+1
        j = i
        thiselem = elem
        while j < c:
            nextelem = l1[(j) % c]    
            a = (thiselem[0],nextelem[0])
            b = (thiselem[1],nextelem[1])
            tot.append((a,b))
            j = j+1
    return tot


class MoviesSimilarities(MRJob):

    def steps(self):
        return [MRStep(mapper=self.mapper,
                       combiner = self.combiner,
                       reducer=self.reducer)]

    def mapper(self, key, line): #group_by_user_rating

        user_id, item_id, rating = line.split(',')
        yield  user_id, (item_id, float(rating))

    def combiner(self, user_id, values): #get_pairwise_items
        item_count = 0
        item_sum = 0
        final = []
        #movie_final = list()
        #movie_ratings = list()
        for item_id, rating in values:
            item_count += 1
            item_sum += rating
            final.append((item_id, rating))
            
        rat = expand_ratings(final)
        for item1, item2 in rat:
            yield (item1[0], item1[1]), \
                    (item2[0], item2[1])

    def reducer(self, user_id, values): #pairwise_items_similarity
        movieA = []
        movieB = []
        item1,item2 = user_id
        for val in values:
            movieA.append(val[0])
            movieB.append(val[1])
        Cosine_Similarity = cosine(movieA,movieB)
        Jaccard_Similarity = round(jaccard(movieA,movieB) , 5)
        Generalized_Jaccard_Similarity =round(generalized_jaccard(movieA,movieB),5)
        Pearson_Correlation = round(pearson_correlation(movieA,movieB) , 5)
        Normalized_Pearson_Correlation =round(normalized_pearson_correlation(movieA,movieB) , 5)
        
        yield (item1,item2),(Jaccard_Similarity,Generalized_Jaccard_Similarity,	Cosine_Similarity, Pearson_Correlation,Normalized_Pearson_Correlation)
        
if __name__ == '__main__':
    MoviesSimilarities.run()
