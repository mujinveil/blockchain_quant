# encoding="utf-8"
import json
import time
from threading import Lock, Timer
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener
from unidecode import unidecode
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from tools.databasePool import POOL


class listener(StreamListener):
    data = []
    lock = None

    def __init__(self, lock):
        # create lock
        self.lock = lock
        # init timer for database save
        self.save_in_database()
        # call __inint__ of super class
        super().__init__()

    def save_in_database(self):
        # set a timer (1 second)
        Timer(1, self.save_in_database).start()
        # with lock, if there's data, save in transaction using one bulk query
        with self.lock:
            if len(self.data):
                insertsql = "INSERT INTO twitter_stream(time_vs,tweet,sentiment) VALUES(%s, %s, %s) "
                cur.executemany(insertsql, self.data)
                conn.commit()
                self.data = []

    def on_data(self, data):
        try:
            # print('data')
            data = json.loads(data)
            # there are records like that:
            # {'limit': {'track': 14667, 'timestamp_ms': '1520216832822'}}
            if 'truncated' not in data:
                # print(data)
                return True
            if data['truncated']:
                tweet = unidecode(data['extended_tweet']['full_text'])
            else:
                tweet = unidecode(data['text'])
            time_ms = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(data['timestamp_ms']) / 1000))
            vs = analyzer.polarity_scores(tweet)
            sentiment = vs['compound']
            # append to data list (to be saved every 1 second)
            with self.lock:
                if 'ethereum' in tweet.lower():
                    print(time_ms, tweet, sentiment)
                    self.data.append((time_ms, tweet, sentiment))

        except KeyError as e:
            # print(data)
            print(str(e))
        return True

    def on_error(self, status):
        print(status)


def collect_tweets(consumer_key, consumer_secret, access_token_key, access_token_secret):
    try:
        lock = Lock()
        auth = OAuthHandler(consumer_key, consumer_secret)
        auth.set_access_token(access_token_key, access_token_secret)
        twitterStream = Stream(auth, listener(lock))
        twitterStream.filter(track=["ethereum"])
    except Exception as e:
        print(e)
        collect_tweets(consumer_key, consumer_secret, access_token_key, access_token_secret)


if __name__ == "__main__":
    # sentimentdatalist = r0.hvals("Sentiment_Strategy")
    # sentimentdatalist = [json.loads(i) for i in sentimentdatalist]
    # if sentimentdatalist:
    ckey = "QG4kEAhNnqQAZ5DMnCxOa9MJk"
    csecret = "8DlPtLvMHehu3ULe7k6oUQGY9ngk7Be0UNsOUOvcsb3a4mUFPd"
    atoken = "764487385069608960-vKAW7gm7hkb9hxSsT0Ne9xybYhkHQQT"
    asecret = "xDYG0mIfDtzDRCabMVS8davPlMVOkJIFoDdlqz40XHGXi"
    analyzer = SentimentIntensityAnalyzer()
    conn = POOL.connection()
    cur = conn.cursor()
    collect_tweets(ckey, csecret, atoken, asecret)
