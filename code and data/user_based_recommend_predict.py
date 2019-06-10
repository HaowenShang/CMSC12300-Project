'''
CMSC 12300 Spring 19

Project: Item-based and User-based Movie Recommendation using MapReduce and Hadoop

Algorithm 2: User-based Recommendation

Team Name: 0errors0warnings
Team Members: Boyang Qu, Ruixi Li, Tianxin Zheng, Haowen Shang
Instructior: Professor Matthew Wachs

Execute the file:
   $ python3 user_based_recommend_predict.py training_ratings.csv user_10nearestneighbor.csv user_movie_prediction.csv
'''

import sys
import math
import os
import csv
import re

class Collaborate_Filter:
    def __init__(self, uu_dataset, ii_dataset, user_id, movie, k):
        self.user_id = user_id
        self.movie = movie
        self.k = k
        self.uu_dataset = uu_dataset
        self.ii_dataset = ii_dataset

# Predict
def predict(user_id, movie, sim_ratings, sim_users, uu_dataset):
    '''
    Make prediction for user on one movie.
    '''
    if not len(sim_ratings):
        return 0.0
    top_result = 0.0
    bottom_result = 0.0
    for neighbor_id, neighbor_similarity in zip(sim_users, sim_ratings):
        neighbor_similarity = float(neighbor_similarity)
        if neighbor_id[1:-1] in uu_dataset:
            rating_all = uu_dataset[neighbor_id[1:-1]]
            if rating_all != 0 and movie in rating_all:
                rating = rating_all[movie]
                top_result += neighbor_similarity * float(rating)
                bottom_result += neighbor_similarity
    if bottom_result:
        return top_result/bottom_result
    else:
        return 0.0


# Helper Functions
def load_data(input_file_name):
    '''
    Load data and return three outputs for extention purpose
    '''
    input_file = open(input_file_name, 'r')
    dataset = []
    uu_dataset = {}
    ii_dataset = {}
    for line in input_file:
        row = str(line)
        row = row.split(",")
        dataset.append(row)

        uu_dataset.setdefault(row[0], {})
        uu_dataset[row[0]].setdefault(row[1], float(row[2]))

        ii_dataset.setdefault(row[1], {})
        ii_dataset[row[1]].setdefault(row[0], float(row[2]))

    allmovie = set()
    for movie in ii_dataset.keys():
        allmovie.add(movie)
    return uu_dataset, ii_dataset, allmovie

def gen_movie_sim_pair(input_file_name, simpair_filename, output_file_name):
    '''
    Make recommendation based on the movies user watched for all users, 
    generate a csv file for training and testing.
    '''
    uu_dataset, ii_dataset, allmovie = load_data(input_file_name)
    simpair_file = open(simpair_filename, 'r')
    with open(output_file_name, mode='w') as csvfile:
        writer = csv.writer(csvfile, delimiter=',')
        for line in simpair_file:
            user_id, simpairs = line.split('\t')
            sim_ratings = re.findall('\\d\\.\\d', simpairs)
            sim_users = re.findall('"\\d+"', simpairs)
            for movie in allmovie:
                if movie not in uu_dataset.get(user_id[1:-1], []): ### KeyError: '\\ufeff1'
                    prediction = predict(user_id, movie, sim_ratings, sim_users, uu_dataset)
                    if prediction:
                        writer.writerow([user_id[1:-1], movie, prediction])

if __name__ == '__main__':
    input_file_name = sys.argv[1]   # test.csv
    simpair_filename = sys.argv[2]   # simpair.csv
    output_file_name = sys.argv[3]    # output.csv

    gen_movie_sim_pair(input_file_name, simpair_filename, output_file_name)
