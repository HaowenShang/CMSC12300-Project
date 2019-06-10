# Item-based and User-based Movie Recommendation using Hadoop MapReduce
This is a repository for CMSC 12300 project. Our project goal is to construct a recommendation system which predicts the rating that the user would give to a movie. We use both item-based algorithm and user-based algorithm for recommendation.

## Files in the code and data folder
### code:

`item_based.py`: Item-based MapReduce algorithm.

`user_based_recommend_mapreduce.py`: User-based MapReuduce algorithm.

`user_based_recommend_predict.py`: Make predictions for all the users in user-based algorithm.

`split_dataset.py` : Split the dataset to training and testing dataset for user-based or item-based approach according to command-line argument.

`test_accuracy.py`: Test recommendation accuracy for user-based or item-based approach according to command-line argument.

`recommend_movies.py`: Movie Recommendation for an entered user.

### data: 

`ratings.csv`: dataset containing user_id, movie_id and rating score.

`movies.csv`: dataset matching movie_id and title.

## How to run our algorithm?
 + For testing accuracy purpose, we need to first split the dataset, then run our item_based/user_based algorithm and finally test the prediction accuracy.
 
    step 1: Run `$ python3 split_dataset.py ratings.csv` to split the dataset.
    
    step 2: For user_based approach, run `$ python3 user_based_recommend_mapreduce.py <training_ratings.csv> user_10nearestneighbor.csv` and then`$ python3 user_based_recommend_predict.py training_ratings.csv user_10nearestneighbor.csv user_movie_prediction.csv`; for item_based approach, run `python3 item_based.py <training_ratings.csv> output_item_based.csv`.
    
    step 3: For user_based approach: run `$ python3 test_accuracy.py out_training.csv user`; for item_based approach: `$ python3 test_accuracy.py out_training.csv item`.
    
+ For making recommendation purpose, we need to run our item_based/user_based algorithm and then the making recommendation algorithm.

  step 1: For user_based approach, run `$ python3 user_based_recommend_mapreduce.py <training_ratings.csv> user_10nearestneighbor.csv` and then`$ python3 user_based_recommend_predict.py training_ratings.csv user_10nearestneighbor.csv user_movie_prediction.csv`; for item_based approach, run `python3 item_based.py <training_ratings.csv> output_item_based.csv`.
  
  step 2: Run `$ python3 recommend_movies.py output.csv` to make recommendations.


## Hypothesis

1. Construct a recommendation system
2. Implement two algorithms
   + Item-based recommendation
   + User-based recommendation
3. Check accuracy
   + Split into training and testing sets
   +  Cross validation
   +  Calculate accuracy score
4. Make accurate movie recommendations!
## Dataset
27,753,444 ratings from 283,228 users on 58,098 movies from MovieLens ml-lastest-small dataset. Below is the rating distribution:
![alt text]( )
<img src="https://raw.githubusercontent.com/TianxinZheng/CMSC12300-project/master/images/data.png" alt="drawing" width="400" height="300" style="float: right;"/>
## Algorithms
Item-based: 3-step MapReduce
![alt text](https://raw.githubusercontent.com/TianxinZheng/CMSC12300-project/master/images/item.png "Logo Title Text 1")
* Firstly, we want to find all movies and their ratings watched by each person. We will use a mapper to extract user and (movie, rating) pair and use a reducer to group all (movie, rating) pair by user.

![alt text](https://raw.githubusercontent.com/TianxinZheng/CMSC12300-project/master/images/i1.png "Logo Title Text 1")
* Secondly, we want to get every pair of movies that were watched by same person and its corresponding rating pair. We will use a mapper to get key-value pair looks like: (movie1, movie2) - (rating1, rating2). Then we want to measure similarities between each movie pair. We use a reducer to compute rating-based similarity between each movie pair and get its similarity scores (movie1, movie2) – (similarity scores, number of person who watched both).

![alt text](https://raw.githubusercontent.com/TianxinZheng/CMSC12300-project/master/images/i2.png "Logo Title Text 1")

* Thirdly,  we will get the output with movies followed by a list of similar movies.

![alt text](https://raw.githubusercontent.com/TianxinZheng/CMSC12300-project/master/images/i3.png "Logo Title Text 1")
User-based: 4-step MapReduce
![alt text](https://raw.githubusercontent.com/TianxinZheng/CMSC12300-project/master/images/user.png "Logo Title Text 1")
* First, to find all movies and users who watched the movie and rated it, we used a mapper to extract movie and (user, rating) pair and a reducer to group all (user, rating) pairs by the movie (Figure 2a).

![alt text](https://raw.githubusercontent.com/TianxinZheng/CMSC12300-project/master/images/u1.png "Logo Title Text 1")
* Second, to get every pair of users who watched the same movie and the corresponding rating pairs, we employed a mapper to get key-value pair, which looked like: (user1, user2) - (rating1, rating2).  Then, we used a reducer to compute the rating-based similarity between each user pair and got its similarity scores (Figure 2b). 
![alt text](https://raw.githubusercontent.com/TianxinZheng/CMSC12300-project/master/images/u2.png "Logo Title Text 1")
* Third, to measure the similarities between each user pair, we used a reducer to compute the rating-based similarity between each user pair and got its similarity scores (Figure 2c). The yielded output was in the format of: user1, user2, similarity scores, (number of movies they both watched).
![alt text](https://raw.githubusercontent.com/TianxinZheng/CMSC12300-project/master/images/u3.png "Logo Title Text 1")
* The final step was to map the users and generate a list of similar users sorted by similarity scores (Figure 2d). The output obtained from the mapreduce steps in the user-based algorithm is in the format of: "user1" [ ("user2", "similarity (From 0 to 1)"), ("user3", "similarity (From 0 to 1)")]. 
![alt text](https://raw.githubusercontent.com/TianxinZheng/CMSC12300-project/master/images/u4.png "Logo Title Text 1")
 
* Prediction: After completing the MapReduce task, we made a prediction for each movie for each user by calculating a simple weighted average of the ratings provided by the k most similar users. Specifically, we only included similar users who rated this movie. We used the following formula, where Wi,1 is the similarity of user i with the k ( we will choose 10 here)  most similar users. We applied this prediction algorithm to all unrated movies by each user.

     ![alt text](https://raw.githubusercontent.com/TianxinZheng/CMSC12300-project/master/images/pre.jpg "Logo Title Text 1")

## Results

![alt text](https://raw.githubusercontent.com/TianxinZheng/CMSC12300-project/master/images/res.png "Logo Title Text 1")






