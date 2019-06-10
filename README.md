# Item-based and User-based Movie Recommendation using Hadoop MapReduce
This is a repository for CMSC 12300 project. Our project goal is to construct a recommendation system which predicts the rating that the user would give to a movie. We use both item-based algorithm and user-based algorithm for recommendation.

## Group Members
Team members: Boyang Qu, Ruixi Li, Tianxin Zheng, Haowen Shang

Advisor: Dr. Matthew Wachs

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

<img src="https://raw.githubusercontent.com/TianxinZheng/CMSC12300-project/master/images/data.png" alt="drawing" width="400" height="300" style="float: right"/>

## Algorithms
Item-based: 3-step MapReduce

<img src="https://raw.githubusercontent.com/TianxinZheng/CMSC12300-project/master/images/item.png" alt="drawing" width="400" height="300" style="float: right;"/>

* Firstly, we want to find all movies and their ratings watched by each person. We will use a mapper to extract user and (movie, rating) pair and use a reducer to group all (movie, rating) pair by user.

<img src="https://raw.githubusercontent.com/TianxinZheng/CMSC12300-project/master/images/i1.png" alt="drawing" width="400" height="300" style="float: right;"/>

* Secondly, we want to get every pair of movies that were watched by same person and its corresponding rating pair. We will use a mapper to get key-value pair looks like: (movie1, movie2) - (rating1, rating2). Then we want to measure similarities between each movie pair. We use a reducer to compute rating-based similarity between each movie pair and get its similarity scores (movie1, movie2) – (similarity scores, number of person who watched both).

<img src="https://raw.githubusercontent.com/TianxinZheng/CMSC12300-project/master/images/i2.png" alt="drawing" width="400" height="300" style="float: right;"/>

* Thirdly,  we will get the output with movies followed by a list of similar movies.

<img src="https://raw.githubusercontent.com/TianxinZheng/CMSC12300-project/master/images/i3.png" alt="drawing" width="400" height="300" style="float: right;"/>

User-based: 4-step MapReduce

<img src="https://raw.githubusercontent.com/TianxinZheng/CMSC12300-project/master/images/user.png" alt="drawing" width="400" height="300" style="float: right;"/>

* First, to find all movies and users who watched the movie and rated it, we used a mapper to extract movie and (user, rating) pair and a reducer to group all (user, rating) pairs by the movie (Figure 2a).

<img src="https://raw.githubusercontent.com/TianxinZheng/CMSC12300-project/master/images/u1.png" alt="drawing" width="400" height="300" style="float: right;"/>
* Second, to get every pair of users who watched the same movie and the corresponding rating pairs, we employed a mapper to get key-value pair, which looked like: (user1, user2) - (rating1, rating2).  Then, we used a reducer to compute the rating-based similarity between each user pair and got its similarity scores (Figure 2b). 

<img src="https://raw.githubusercontent.com/TianxinZheng/CMSC12300-project/master/images/u2.png" alt="drawing" width="400" height="300" style="float: right;"/>

* Third, to measure the similarities between each user pair, we used a reducer to compute the rating-based similarity between each user pair and got its similarity scores (Figure 2c). The yielded output was in the format of: user1, user2, similarity scores, (number of movies they both watched).

<img src="https://raw.githubusercontent.com/TianxinZheng/CMSC12300-project/master/images/u3.png" alt="drawing" width="400" height="200" style="float: right;"/>

* The final step was to map the users and generate a list of similar users sorted by similarity scores (Figure 2d). The output obtained from the mapreduce steps in the user-based algorithm is in the format of: "user1" [ ("user2", "similarity (From 0 to 1)"), ("user3", "similarity (From 0 to 1)")]. 

<img src="https://raw.githubusercontent.com/TianxinZheng/CMSC12300-project/master/images/u4.png" alt="drawing" width="400" height="150" style="float: right;"/>
 
* Prediction: After completing the MapReduce task, we made a prediction for each movie for each user by calculating a simple weighted average of the ratings provided by the k most similar users. Specifically, we only included similar users who rated this movie. We used the following formula, where Wi,1 is the similarity of user i with the k ( we will choose 10 here)  most similar users. We applied this prediction algorithm to all unrated movies by each user.

    <img src="https://raw.githubusercontent.com/TianxinZheng/CMSC12300-project/master/images/pre.jpg" alt="drawing" width="250" height="50" style="float: right;"/>

## Results

### Cross-Validation
In order to assess the accuracy of our model, we performed cross-validation of both item-based (movie-based) and user-based models. More specifically, a train/test split was done, and then the root mean square error (RMSE) was calculated for the predicted ratings and actual ratings. 

If we want to recommend movies to a user, then we must learn something about past movie ratings by the user. Therefore, a small portion of users was selected as the testing users and a small portion of the movie rated by these users was finally selected as the testing movies in the testing dataset. 1/10 of the users were selected randomly as the testing users, and then ¼ of the movies watched by these users were again selected randomly into the testing dataset. Movie ratings by the remaining users and movie ratings by the testing users on the remaining movies were used as the training dataset. 

After running the two algorithms on their respective training dataset, both item-based approach and user-based approach yielded predicted testing movie ratings by the testing users. Then for each approach, we calculated the root mean square error to measure the model accuracy.  The root mean square error is shown below in Table 1. The RMSE values suggest that with a large dataset, the error is relatively stable at less than 0.5 stars on a 5 star rating-scale. We decided that both the item-based model and the user-based model were effective in making accurate movie recommendation for users, and the item-based model was slightly better with lower RMSE values in predicted ratings. 

<img src="https://raw.githubusercontent.com/TianxinZheng/CMSC12300-project/master/images/res.png" alt="drawing" width="600" height="300" style="float: right"/>

### Make Recommendation
Both item-based and user-based approach yielded predicted ratings for unwatched movies by the user. Furthermore, we validated that the predicted ratings from both approaches were relatively accurate. Thus, we could conveniently recommend to a user the movies with the highest expected ratings from him/her. In conclusion, our models were able to generate accurate recommendations to users based on either movie similarities or user behaviors. 


## Acknowledgement
Thanks for Dr. Matthew Waches for his helpful comments and suggestions.



