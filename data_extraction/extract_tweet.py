import configparser
import csv
import logging
import re
import string
import time
import requests

from datetime import datetime, timedelta
from textblob import TextBlob
from pymongo import MongoClient

from scheduler.scheduler import scheduler


# Total Requests per window.
N = 12

# Setup the configuration file
config = configparser.ConfigParser()
config.read('../config/tweet.cfg')

csv_file = "../data/tweets.csv"

logging.basicConfig(format='%(asctime)s %(message)s',
                    datefmt='%m/%d/%Y %I:%M:%S %p',
                    filename='../logs/tweets.log',
                    level=logging.DEBUG)

Log = logging.getLogger('../logs/tweets.log')

# If scheduler is not running, start the scheduler
if not scheduler.running:
    scheduler.start()

headers = dict()

# Prepare the query parameters
base_url = config['DEFAULT']['BASE_URL']
query_param = config['query']['QUERY_PARAM']
headers['Host'] = config['headers']['HOST']
headers['User-Agent'] = config['headers']['USER_AGENT']
headers['Accept-Encoding'] = config['headers']['ACCEPT_ENCODING']
headers['Authorization'] = config['headers']['AUTHORIZATION']


def tag_sentiment_type(polarity):
    """
    Tag the sentiment for a tweet based on the polarity.
    polarity > 0, tag tweet: positive
    polarity == 0, tag tweet: neutral
    polarity < 0, tag tweet: negative
    """

    if polarity < 0:
        sentiment_type = 'negative'

    elif polarity > 0:
        sentiment_type = 'positive'

    else:
        sentiment_type = 'neutral'

    return sentiment_type


def perform_sentiment_analysis(text):
    """This method perform the sentiment analysis using TextBlob which is built on nltk."""

    try:
        polarity, subjectivity = TextBlob(text).sentiment.polarity, TextBlob(text).sentiment.subjectivity

        sentiment_type = tag_sentiment_type(polarity)

        return polarity, subjectivity, sentiment_type

    except Exception as ex:
        Log.error(str(ex))


def clean_text(text):
    """ This method cleans up the data and returns the str """

    text = text.lower().strip()
    text = re.sub(r'\[.*?\]', '', text)
    text = re.sub(r'[%s]' % re.escape(string.punctuation), '', text)
    text = re.sub(r'\n', '', text)
    text = re.sub(r'\w  *\d+\w*', '', text)
    text = re.sub(r'https:\/\//s+ | http:\/\//s+', '', text)
    text = re.sub(r'rt\s+', '', text)
    text = text.replace('@', '')
    text = text.replace('#', '')
    text = text.encode('ascii', 'ignore').decode('ascii')
    return text


def write_to_csv(documents):
    """
    This method writes the documents to csv file.
    """
    with open(csv_file, "a+") as fd:
        writer = csv.writer(fd)
        writer.writerows(documents)


def insert_headers():
    row_names = [['created_at', 'id', 'tweet', 'name', 'description', 'location', 'followers_count', 'friends_count',
                  'favourites_count', 'statuses_count', 'is_retweet', 'r_name', 'r_description', 'r_location',
                  'polarity', 'subjectivity', 'emotion']]

    # Write headers to CSV.
    write_to_csv(row_names)


def collect_documents(result, store_type):
    insert_documents = list()
    id_list = list()
    for document in result:
        id_list.append(document.get('id'))

        if store_type == 'csv':
            try:
                insert_data = list()
                insert_data.append(document.get('created_at'))
                insert_data.append(document.get('id'))
                full_text = clean_text(document.get('full_text'))
                insert_data.append(full_text)
                insert_data.append(document['user'].get('name'))
                insert_data.append(document['user'].get('description'))
                insert_data.append(document['user'].get('location'))
                insert_data.append(document['user'].get('followers_count'))
                insert_data.append(document['user'].get('friends_count'))
                insert_data.append(document['user'].get('favourites_count'))
                insert_data.append(document['user'].get('statuses_count'))

                if document.get('retweeted_status', False):
                    insert_data.append("retweet")
                    retweet_status = document.get('retweeted_status')
                    insert_data.append(retweet_status['user'].get('name'))
                    insert_data.append(retweet_status['user'].get('description'))
                    insert_data.append(retweet_status['user'].get('location'))
                else:
                    # the tweet found was not retweeted, so no retweet infomation.
                    insert_data.extend(["tweet", "", "", ""])

                polarity, subjectivity, sent_type = perform_sentiment_analysis(full_text)
                insert_data.append(polarity)
                insert_data.append(subjectivity)
                insert_data.append(sent_type)

                insert_documents.append(insert_data)
            except Exception as ex:
                Log.error(str(ex))
                continue

        elif store_type == 'mongodb':
            try:
                full_text = clean_text(document.get('full_text'))
                polarity, subjectivity, sent_type = perform_sentiment_analysis(full_text)

                insert_data = {
                    "created_at": document.get('created_at'),
                    "id": document.get('id'),
                    "tweet": full_text,
                    "name": document['user'].get('name'),
                    "location": document['user'].get('location'),
                    "followers_count": document['user'].get('followers_count'),
                    "polarity": polarity,
                    "subjectivity": subjectivity,
                    "emotion": sent_type
                }
                insert_documents.append(insert_data)

            except Exception as ex:
                Log.error(str(ex))
                continue

    yield insert_documents, id_list


def collect_recent_tweets(**kwargs):
    """
    This method collects the recent tweets for the following search tags
    - Ireland
    - Virus
    - corona virus
    """
    Log.debug("Start Collecting recent tweets.")
    max_id = 0
    store_type, records = kwargs.get('store_type'), kwargs.get('records')
    for _ in range(N):
        try:
            Log.info("MAX ID: %s", str(max_id))

            if max_id:
                result = requests.get(url=base_url + 'max_id=' + str(max_id-1) + '&' + query_param,
                                      headers=headers).json()
            else:
                result = requests.get(url=base_url + query_param, headers=headers).json()

            result = result.get('statuses')

            # Insert the documents into collection `recent_tweets`
            for documents, id_list in collect_documents(result, store_type):
                if store_type == 'csv':
                    write_to_csv(documents)

                elif store_type == 'mongodb':
                    records.insert_many(documents)

                max_id = min(id_list)
                Log.info("Minimum Id of the tweet: %s", str(max_id))

        except Exception as ex:
            Log.error("Error while collecting logs. %s", str(ex))

    Log.debug("Successfully collected recent tweets.")


# Create the MongoDB connector to establish the connection to the MongoDB Altas
class MongoDBConnector:
    def __init__(self):
        self.client = MongoClient(config['mongo.client']['MONGO_CLIENT'])
        self.records = self.client.twitter_db.recent_tweets


class ExtractTweets(MongoDBConnector):
    def __init__(self):
        self.start_date = datetime.now() + timedelta(seconds=10)
        self.store_type = config['data.store']['store']
        MongoDBConnector.__init__(self)

    def start_scheduler(self):
        while True:
            self.collect_twitter_data()

    def collect_twitter_data(self):
        # Set the schedule interval in configuration file.
        t = config['schedule.param']['SCHEDULE_TIME']
        kwargs = {"store_type": self.store_type, "records": self.records}

        job = scheduler.add_job(collect_recent_tweets, start_date=self.start_date, kwargs=kwargs,
                                trigger='interval', minutes=int(t), max_instances=4, coalesce=True)

        sleep_time = datetime.timestamp(job.next_run_time) - int(time.time())
        print("Next Wakeup Call in {}. Next Run Time {} ".format(sleep_time, job.next_run_time))
        time.sleep(sleep_time)


if config['data.store']['store'] == 'csv':
    insert_headers()

# Start the extraction and analysis
ExtractTweets().start_scheduler()
