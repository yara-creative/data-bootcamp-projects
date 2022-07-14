#---Scrapes and stores tweets in a MongoDB database---#
# coding: utf-8
import tweepy
from twitter_credentials import BEARER_TOKEN
import logging
from pymongo import MongoClient

# Authenticate Twitter API client
client_t = tweepy.Client(bearer_token=BEARER_TOKEN)
if client_t:
    logging.critical("\n---Connected to Twitter API---\n")
else:
    logging.critical("\n---Verify Twitter API credentials---\n")

# Function to scrape user-specific tweets:
# def scrape_tweets(user_handle):
#   user_handle = client.get_user(username = user_handle, user_fields = ['name', 'id', 'created_at'])
#   user = user_handle.data
#   tweet_scraper = tweepy.Paginator(
#       method = client_t.get_users_tweets,
#       id = user.id,
#       exclude = ['replies', 'retweets'],
#       tweet_fields = ['author_id', 'created_at', 'public_metrics']).flatten(limit=100)
#
#   tweets_list = []
#
#   for tweet in tweet_scraper:
#        tweets = {"user_id":tweet.id,
#                "created_at":tweet.created_at,
#                "retweet_count":tweet.public_metrics["retweet_count"],
#                "reply_count":tweet.public_metrics["reply_count"],
#                "like_count":tweet.public_metrics["like_count"],
#                "quote_count":tweet.public_metrics["quote_count"],
#                "tweet_text":tweet.text}
#        tweets_list.append(tweets)
#
#    return tweets_list

# Function to scrape keyword-specific tweets:
def scrape_tweets(keyword):
    tweet_scraper = tweepy.Paginator(
    method = client_t.search_recent_tweets,
    query = f"{keyword} -is:retweet -is:reply -is:quote -has:links lang:en",
    tweet_fields = ["author_id", "created_at", "public_metrics"]).flatten(limit = 100)

    tweets_list = []

    for tweet in tweet_scraper:
        tweets = {"user_id":tweet.id,
                  "created_at":tweet.created_at,
                  "retweet_count":tweet.public_metrics["retweet_count"],
                  "reply_count":tweet.public_metrics["reply_count"],
                  "like_count":tweet.public_metrics["like_count"],
                  "quote_count":tweet.public_metrics["quote_count"],
                  "tweet_text":tweet.text}
        tweets_list.append(tweets)

    return tweets_list

# Scrape tweets
tweets_list = scrape_tweets('Ukraine')

# Set up MongoDB client
CONTAINER_NAME = "mongodb"
PORT = 27017
conn_string_m = f"mongodb://{CONTAINER_NAME}:{PORT}" # For MongoDB installed on Docker, not locally

# Authenticate MongoDB client
client_m = MongoClient(conn_string_m)
if client_m:
    logging.critical("\n---Connected to MongoDB server---\n")
else:
    logging.critical("\n---Verify MongoDB credentials---\n")

# Create MongoDB database and store tweets in it
db = client_m.twitter
db.twitter.insert_many(tweets_list)
