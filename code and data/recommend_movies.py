'''
CMSC 12300 Spring 19

Project: Item-based and User-based Movie Recommendation using MapReduce and Hadoop

Movie Recommendation for an entered user.

Team Name: 0errors0warnings
Team Members: Boyang Qu, Ruixi Li, Tianxin Zheng, Haowen Shang
Instructior: Professor Matthew Wachs

Execute the file:
	For user_based or item-based approach:
	 $ python3 recommend_movies.py output.csv

'''

import sys
import csv


if __name__ == '__main__':
	# Collect information.
	approach = input("Please enter the algorithm for recommendation (user-based/movie-based): ")
	user = input("Please enter the user to make recommendation: ")
	number = input("Please enter number of recommended movies: ")
	
	# Correlate movie_id with movie names.
	recommendations = []
	name_dict = {}
	with open("movies.csv") as name_file:
		movie_name = csv.reader(name_file, delimiter = ',')
		for row in movie_name:
			if len(row) != 0:
				name_dict[row[0]] = row[1]
	
	# Item-based algorithm.
	if approach == "movie-based":
		# Read similarity csv file.
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

		# Obtain rated movies by the user.
		user_ratings = {}
		with open('ratings.csv') as rate_file:
			user_rates = csv.reader(rate_file, delimiter = ',')
			for row in user_rates:
				if row[0] == user:
					user_ratings[row[1]] = row[2]

		# Calculate weighted similarity as predicted ratings for each unwatched movie.
		predictions = {}
		for movie in user_ratings:
			if movie in movie_similarities:
				for unwatched in movie_similarities[movie]:
					if unwatched not in user_ratings:
						if movie_similarities[movie][unwatched][2:4] == "..":
							movie_similarities[movie][unwatched] = movie_similarities[movie][unwatched][0:2] + movie_similarities[movie][unwatched][3:]
						if movie_similarities[movie][unwatched] == "":
							continue
						if unwatched in predictions:
							predictions[unwatched] += float(user_ratings[movie])*float(movie_similarities[movie][unwatched])
						else:
							predictions[unwatched] = float(user_ratings[movie])*float(movie_similarities[movie][unwatched])

		# Sort movies according to predicted ratings.
		recommendations = [i[0] for i in sorted(predictions.items(), key = lambda x: -x[1])]

	# User-based algorithm.
	if approach == "user-based":
			# Obtain predicted ratings.
			predicted_ratings = {}
			with open(sys.argv[1]) as pred_file:
				pred_ratings = csv.reader(pred_file, delimiter = ',')
				for row in pred_ratings:
					if len(row) == 0 or row[0] != user:
						continue
					predicted_ratings[row[1]] = row[2]

			# Sort movies according to predicted ratings.
			recommendations = [i[0] for i in sorted(predicted_ratings.items(), key = lambda x: -float(x[1]))]


	# Convert movie_id to movie_name and print.
	rcd_movie = []
	if len(recommendations) >= int(number):
		rcd_movie = recommendations[:int(number)]
	else:
		if len(recommendations) == 0:
			print("Sorry, no recommend movie for this user (did not review any movies or reviewed movies not watched by other users.")
		else:
			rcd_movie = recommendations
			print("Sorry, only ", len(recommendations), "movies to recommend.")

	for ind, movie_id in enumerate(rcd_movie, 0):
		rcd_movie[ind] = name_dict[movie_id]

	if len(rcd_movie) != 0:
		print(rcd_movie)


			


