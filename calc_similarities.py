import math
import numpy as np
from scipy.sparse import csr_matrix, lil_matrix
from collections import Counter

def norm(vector):
    return np.linalg.norm(vector)

# Cosine Similarity
def cosine(v1, v2):
    numerator = np.dot(csr_matrix(v1).toarray(),csr_matrix(v2).T.toarray())
    denominator = (norm(v1) * norm(v2))
    return ((numerator / denominator).tolist()[0][0] if denominator else 0.0)

# Jaccard Similarity
def jaccard(v1,v2):
    countlistA = Counter(v1)
    countlistB = Counter(v2)
    intersection_of_two_vectors = len(list((countlistB & countlistA).elements()))
    union_of_two_vectors = len(v1) + len(v2)
    return (intersection_of_two_vectors / float(union_of_two_vectors)) if union_of_two_vectors else 0.0

# Generalized Jaccard Similarity
def generalized_jaccard(v1,v2):
    sum_min = sum_max = 0.0
    for x,y in zip(v1,v2):
        sum_min +=  min(x,y)
        sum_max +=  max(x,y)
    return sum_min / sum_max if sum_max else 0.0

# Pearson's Correlation 
def pearson_correlation(v1,v2):
    n = len(v1)
    sigma_xy = np.dot(csr_matrix(v1).toarray(),csr_matrix(v2).T.toarray())
    sigma_x = sum(v1)
    sigma_y = sum(v2)
    sigma_x2 = norm(v1) * norm(v1)
    sigma_y2 = norm(v2) * norm(v2)
    numerator = (n*sigma_xy - (sigma_x * sigma_y))
    denominator1 = (n*sigma_x2) - (sigma_x * sigma_x)
    denominator2 = (n*sigma_y2) - (sigma_y * sigma_y)
    if denominator1 > 0.0:
        denominator1 = math.sqrt(denominator1)
    else:
        denominator1 = 0.0
    if denominator2 > 0.0:
        denominator2 = math.sqrt(denominator2)
    else:
        denominator2 = 0.0
    denominator = denominator1 * denominator2
    return (numerator / denominator).tolist()[0][0] if denominator else 0.0

def normalized_pearson_correlation(v1,v2):
    return (pearson_correlation(v1,v2) + 1.0) / 2.0 

