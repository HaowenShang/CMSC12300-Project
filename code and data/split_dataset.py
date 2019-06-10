'''
CMSC 12300 Spring 19

Project: Item-based and User-based Movie Recommendation using MapReduce and Hadoop

Split the dataset to training and testing dataset.

Team Name: 0errors0warnings
Team Members: Boyang Qu, Ruixi Li, Tianxin Zheng, Haowen Shang
Instructior: Professor Matthew Wachs

Execute the file:
	 $ python3 split_dataset.py ratings.csv
'''

import random
import csv
import sys

if __name__ == '__main__':
	# Read all users' ratings from csv file in to 
	# a dictionary (user as key) of dictionaries (movie as key and rating as value).
	# Randomly select 1/10 of the users as the testing dataset.
	all_user_rates = dict()
	with open(sys.argv[1]) as rating_file:
		all_ratings = csv.reader(rating_file, delimiter = ',')
		for row in all_ratings:
			if row[0] in all_user_rates:
				all_user_rates[row[0]][row[1]] = row[2]
			else:
				all_user_rates[row[0]] = dict()
				all_user_rates[row[0]] = {row[1]: row[2]}
	all_users = all_user_rates.keys()
	# Randomly select test users.
	testing_users = random.sample(list(all_users), int(len(all_users)/10))


	# Randomly select test movies.
	# Write in to testing and traing csv files
	testing_rates = {}
	for user in testing_users:
		testing_rates[user] = {}
		test_movies = random.sample(list(all_user_rates[user].keys()), int(len(all_user_rates[user])/8))
		for movie in test_movies:
			testing_rates[user][movie] = all_user_rates[user][movie]
			del all_user_rates[user][movie]

	with open('testing_ratings.csv', mode = 'w') as test_file:
		test_writer = csv.writer(test_file)
		for test_user in testing_rates:
			for test_movie in list(testing_rates[test_user].keys()):
				test_writer.writerow([test_user, test_movie, testing_rates[test_user][test_movie]])

	with open('training_ratings.csv', mode = 'w') as train_file:
		train_writer = csv.writer(train_file)
		for train_user in all_user_rates:
			for train_movie in list(all_user_rates[train_user].keys()):
				train_writer.writerow([train_user, train_movie, all_user_rates[train_user][train_movie]])


