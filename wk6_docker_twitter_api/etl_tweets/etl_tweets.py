#---Extracts tweets from MongoDB, Transforms tweets into sentiment scores, Loads scores into Postgres database
# coding: utf-8
from pymongo import MongoClient
from sqlalchemy import create_engine
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from postgres_credentials import PG_USERNAME, PG_PASSWORD
import logging

# Set up MongoDB client
HOST_MDB = "mongodb" # if in docker it would be the container name
PORT_MDB = 27017
# Connect to MongoDB server
conn_string_m = f"mongodb://{HOST_MDB}:{PORT_MDB}"
client_m = MongoClient(conn_string_m)

# Get tweets
db_tweets = client_m.twitter
# Connect to tweets collection
tweets_collection = db_songs.collection_tweets

# Set up PostgreSQL client
USERNAME_PG = PG_USERNAME
PASSWORD_PG = PG_PASSWORD
HOST_PG = "postgresdb"
PORT_PG = 5432
DATABASE_NAME_PG = "tweets_table"

# Connect to Postgres
conn_string_p = f"postgresql://{USERNAME_PG}:{PASSWORD_PG}@{HOST_PG}:{PORT_PG}/{DATABASE_NAME_PG}"
p = create_engine(conn_string_p)

# Create Postgres table
pg.execute("""
CREATE TABLE IF NOT EXISTS tweets_table (
    id BIGSERIAL PRIMARY KEY,
    tweet_text VARCHAR(400),
    like_count VARCHAR(100),
    retweet_count VARCHAR(100),
    sentiment NUMERIC,
    pos_neu_neg VARCHAR(100)
);

""")

# Extract - get tweet from MongoDB database
def extract():
    """
    Gets list of tweets from MongoDB database.
    """
    tweets_document_list = list(tweets_collection.find())

    return tweets_document_list

# Transform - get sentiment for tweet
def transform(tweet):
    """
    Get the sentiment score and its interpretation (positive/neutral/negative) for each tweet.
    """
    tweet_text = tweet["tweet_text"]
    like_count = tweet["like_count"]
    retweet_count = tweet["retweet_count"]

    s = SentimentIntensityAnalyzer()
    scores = s.polarity_scores(tweet_text)
    sentiment_score = scores["compound"]

    if sentiment > 0.05:
        pos_neu_neg = "positive."
    elif sentiment < -0.05:
        pos_neu_neg = "negative."
    else:
        pos_neu_neg = "neutral."

    logging.critical("\n---Tweet transformed into sentiment score---\n")
    return tweet_text, like_count, retweet_count, sentiment_score, pos_neu_neg

# Load - add tweet to postgres database
def load(tweet_text, sentiment_score, pos_neu_neg):
    """
    Write the transformed tweets into a postgres database.
    """
    if pg.execute(f"""
    INSERT INTO tweets_table (tweet_text, like_count, retweet_count, sentiment_score, pos_neu_neg)
    VALUES ("{tweet_text}", "{like_count}", "{retweet_count}", "{sentiment_score}", "{pos_neu_neg}");
    """
    ):
        logging.critical(f"\n---Tweet and sentiment score {sentiment_score} loaded into Postgres---\n")

# Iterate over each tweet to perform ETL
for tweet_document in extract():
    logging.critical(f"\n---Tweet extracted---\n{tweet_document}")
    tweet_text, sentiment_score, pos_neu_neg = transform(tweet_document)
    load(tweet_text, like_count, retweet_count, sentiment_score, pos_neu_neg)
