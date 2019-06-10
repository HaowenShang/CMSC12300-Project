'''
CMSC 12300 Spring 19

Project: Item-based and User-based Movie Recommendation using MapReduce and Hadoop

Test recommendation accuracy for user-based or item-based approach according to command-line argument.

Team Name: 0errors0warnings
Team Members: Boyang Qu, Ruixi Li, Tianxin Zheng, Haowen Shang
Instructior: Professor Matthew Wachs

Execute the file:
	For user_based approach:
	 $ python3 test_accuracy.py out_training.csv user
	 or
	 $ python3 test_accuracy.py out_training.csv item
'''

import random
import csv
import sys

if __name__ == '__main__':
	
	# Item-based approach testing
	if sys.argv[2] == "item":
		total_test_movies = 0
		mse_total = 0.0
		
		# Read movie similarities from a csv file into a dictionary (movie 1 as key)
		# of dictionaries (movie 2 as key and similarity as value).
		movie_similarities = {}
		with open(sys.argv[1]) as sim_file:
			movie_sim = csv.reader(sim_file, delimiter = ',')
			for row in movie_sim:
				if(len(row) == 0):
					continue
				user_id = row[0][0:row[0].find('\t')]
				movie_id = row[0][row[0].find('\t')+3: len(row[0])-1]
				if user_id in movie_similarities:
					movie_similarities[user_id][movie_id] = row[1]
				else:
					movie_similarities[user_id] = dict()
					movie_similarities[user_id] = {movie_id: row[1]} 

		# Read testing dataset ratings.
		actual_ratings = {}
		with open('testing_ratings.csv') as user_file:
			test_ratings = csv.reader(user_file, delimiter = ',')
			for row in test_ratings:
				if row[0] in actual_ratings:
					actual_ratings[row[0]][row[1]] = row[2]
				else:
					actual_ratings[row[0]] = {row[1]: row[2]}

		# Read rated movies by tested users in testing dataset.
		train_ratings = {}
		with open('training_ratings.csv') as rated_file:
			train_ratings = csv.reader(user_file, delimiter = ',')
			for row in train_ratings:
				if row[0] in actual_ratings:
					if row[0] in train_ratings:
						train_ratings[row[0]][row[1]] = row[2]
					else:
						train_ratings[row[0]] = {row[1]: row[2]}

		
		# For each user, alculated the predicted ratings for testing dataset.
		for user in actual_ratings.keys():
			# Training movies.
			input_movies = train_ratings[user].keys()
			output_movies = actual_ratings[user].keys()
			
			# Calculate predicted ratings.
			for movie1 in output_movies:
				if movie1 in movie_similarities:
					rated_num = 0
					weighted_similarities[movie1] = 0
					for movie2 in input_movies:
						if movie2 in movie_similarities[movie1]:
							if movie_similarities[movie1][movie2][2:4] == "..":
								movie_similarities[movie1][movie2] = movie_similarities[movie1][movie2][0:2] + movie_similarities[movie1][movie2][3:]
							if(movie_similarities[movie1][movie2] == ""):
								continue
							weighted_similarities[movie1] += float(movie_similarities[movie1][movie2]) * float(training_ratings[user][movie2])
							rated_num += 1
					if rated_num != 0:
						weighted_similarities[movie1] /= rated_num
					else:
						del weighted_similarities[movie1]

			# Calculate RMSE (Root mean square error).
			for movie_name in weighted_similarities:
				total_test_movies += 1
				mse_total += ((float(actual_ratings[user][movie_name]) - float(weighted_similarities[movie_name]))) ** 2

		print("Root Mean Square Error = ", (mse_total/total_test_movies)**0.5)
	
	# User-based approach testing.
	if sys.argv[2] == "user":
		actual_ratings = {}
		with open('testing_ratings.csv') as user_file:
			test_ratings = csv.reader(user_file, delimiter = ',')
			for row in test_ratings:
				if row[0] in actual_ratings:
					actual_ratings[row[0]][row[1]] = row[2]
				else:
					actual_ratings[row[0]] = dict()
					actual_ratings[row[0]] = {row[1]: row[2]}

		predicted_ratings = {}
		with open(sys.argv[1]) as pred_file:
			pred_ratings = csv.reader(pred_file, delimiter = ',')
			for row in pred_ratings:
				if len(row) == 0:
					continue
				if row[0] in actual_ratings and row[1] in actual_ratings[row[0]]:
					if row[0] in predicted_ratings:
						predicted_ratings[row[0]][row[1]] = row[2]
					else:
						predicted_ratings[row[0]] = dict()
						predicted_ratings[row[0]] = {row[1]: row[2]}

		# Calculate mse.
		mse_sum = 0.0
		total_num = 0
		for user in actual_ratings:
			for movie in actual_ratings[user]:
				if user in predicted_ratings and movie in predicted_ratings[user]:
					mse_sum += ((float(actual_ratings[user][movie]) - float(predicted_ratings[user][movie]))) ** 2
					total_num += 1

		print("Root Mean Square Error = ", (mse_sum/total_num)**0.5)
