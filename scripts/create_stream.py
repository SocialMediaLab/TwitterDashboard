from __future__ import unicode_literals
import sys
import nltk
from nltk.corpus import stopwords
from nltk import word_tokenize, pos_tag, ne_chunk
from nltk.tree import Tree
import os
import tweepy
import time
import json
from textblob import TextBlob
import argparse
import thread, time, datetime
import day_caching
import cgi
import re
import pymongo
from nltk.tag import StanfordNERTagger
from nltk.tokenize import word_tokenize
from pprint import pprint
import sys

if len(sys.argv) < 2:
	print("To create stream for hashtag: python create_stream hashtag")
	sys.exit()

stream_name = sys.argv[1]
print("Collecting Tweets for #" + stream_name)

config = json.loads(open('config.json', 'r').read())

def get_sentiment(text):
    blob = TextBlob(text)
    average=0
    for sentence in blob.sentences:
       sent = sentence.sentiment.polarity
       if sent != 0:
          average=(average+sent)/2
    return average*100

def get_sentiment_range(sentiment):
    if sentiment >= 10:
        return ("positive")
    elif sentiment < 0:
        return ("negative")
    else:
        return ("neutral")

def get_subjectivity_range(subjectivity):
    if subjectivity>=10:
        return ("subjective")
    else:
        return ("Non subjective")

def get_wordcounts(text):
    default_stopwords = set(nltk.corpus.stopwords.words('english'))
    all_stopwords = default_stopwords
    words = nltk.word_tokenize(text)
    words = [word for word in words if not word.isnumeric()]
    words = [word.lower() for word in words]
    words = [word for word in words if word != "https"]
    words = [word for word in words if word not in all_stopwords]
    words = [word for word in words if re.match('^[\w]+$',word)]
    words = [word for word in words if len(word) > 2]
    fdist = nltk.FreqDist(words)
    data=fdist.most_common(2)
    if len(data)==1:
        return([data[0], "None"])
    elif len(data)==2:
        return([data[0],data[1]])
    elif len(data)==0:
        return(["None","None"])

def get_continuous_chunks(tagged_sent):
    continuous_chunk = []
    current_chunk = []
    for token, tag in tagged_sent:
        if tag != "O":
            current_chunk.append((token, tag))
        else:
            if current_chunk: # if the current chunk is not empty
                continuous_chunk.append(current_chunk)
                current_chunk = []
    # Flush the final current_chunk into the continuous_chunk, if any.
    if current_chunk:
        continuous_chunk.append(current_chunk)
    return continuous_chunk

def get_namedentities(text):
    st = StanfordNERTagger('/usr/local/stanford-ner/classifiers/english.all.3class.distsim.crf.ser.gz','/usr/local/stanford-ner/stanford-ner.jar', encoding='utf-8')

    tokenized_text = word_tokenize(text)
    ne_tagged_sent = st.tag(tokenized_text)
    named_entities = get_continuous_chunks(ne_tagged_sent)
    named_entities_str = [" ".join([token for token, tag in ne]) for ne in named_entities]
    named_entities_str_tag = [(" ".join([token for token, tag in ne]), ne[0][1]) for ne in named_entities]
    list =[]
    for x,y in named_entities_str_tag:
       if len(x.split())>1 and (x not in list):
          list.append(x.decode('utf-8'))
    if list:
       return list[0]
    return list

def get_subjectivity(text):
    blob = TextBlob(text)
    average=0
    for sentence in blob.sentences:
       sent = sentence.sentiment.subjectivity
       if sent != 0:
          average=(average+sent)/2
    return average*100

def create_events (tweet):
    if len(tweet['entities']['user_mentions']) >= 1:
        mentions1=tweet['entities']['user_mentions'][0]['screen_name']
    else:
        mentions1="None"
    if len(tweet['entities']['user_mentions']) >= 2:
        mentions2=tweet['entities']['user_mentions'][1]['screen_name']
    else:
        mentions2="None"
    if len(tweet['entities']['hashtags']) >= 1:
        hashtag1=tweet['entities']['hashtags'][0]['text'].lower()
    else:
        hashtag1="None"
    if len(tweet['entities']['hashtags']) >= 2:
        hashtag2=tweet['entities']['hashtags'][1]['text'].lower()
    else:
        hashtag2="None"
    if len(tweet['entities']['urls']) == 1:
        urls1=tweet['entities']['urls'][0]['url']
    else:
        urls1="None"
    if len(tweet['entities']['urls']) > 1:
        urls2=tweet['entities']['urls'][1]['url']
    else:
        urls2="None"
    if 'media' in tweet['entities']:
        media = 'true'
    else:
        media= 'false'
    if 'retweeted_status' in tweet:
        retweet_status= 'true'
    else:
        retweet_status = 'false'

    sentiment = get_sentiment(tweet['text'])
    sentiment_range =get_sentiment_range(sentiment)
    subjectivity = get_subjectivity(tweet['text'])
    subjectivity_range =get_subjectivity_range(subjectivity)
    terms=get_wordcounts(tweet['text'])
    named_entities=get_namedentities(tweet['text'].encode('ascii','ignore'))
    #print "ID "+tweet['id_str']
    x = {
      "_id":tweet['id_str'],
      "subjectivity_range":subjectivity_range,
      "sentiment_range":sentiment_range,      
      "text":tweet['text'].encode('ascii','ignore'),
      "username":tweet['user']['screen_name'],
      "hashtag1":hashtag1,
      "hashtag2":hashtag2,
      "created_at": tweet['created_at'],
      "mentions1":mentions1,
      "mentions2":mentions2,
      "urls1":urls1,
      "urls2":urls2,
      "sentiment":sentiment,
      "subjectivity":subjectivity,
      "named_entities":named_entities,
      "retweeted":retweet_status,
      "replied_to":tweet['in_reply_to_screen_name'],
      "media":media
    }

    print('New tweet: ' + '@' + x['username'] + ': ' + x['text'])

    # print x["text"] + '\n'

    queue = collection.find_one({"_id": "QUEUE"})


    if queue == None:
        queue = collection.insert_one({
            "_id":"QUEUE",
            "values": [tweet['id_str']]
        })
    ids = queue['values']

    ids.insert(0, tweet['id_str'])

    if len(ids) >= 100:
        ids = ids[0:100]

    collection.replace_one( {'_id':'QUEUE'},
    {
        "_id":"QUEUE",
        "values": ids
    })

    collection.insert_one(x)

def test_rate_limit(api, wait=True, buffer=.1):
    """
    Tests whether the rate limit of the last request has been reached.
    :param api: The `tweepy` api instance.
    :param wait: A flag indicating whether to wait for the rate limit reset
                 if the rate limit has been reached.
    :param buffer: A buffer time in seconds that is added on to the waiting
                   time as an extra safety margin.
    :return: True if it is ok to proceed with the next request. False otherwise.
    """
    #Get the number of remaining requests
    remaining = int(api.last_response.getheader('x-rate-limit-remaining'))
    #Check if we have reached the limit
    if remaining == 0:
        limit = int(api.last_response.getheader('x-rate-limit-limit'))
        reset = int(api.last_response.getheader('x-rate-limit-reset'))
        #Parse the UTC time
        reset = datetime.fromtimestamp(reset)
        #Let the user know we have reached the rate limit
        print "0 of {} requests remaining until {}.".format(limit, reset)

        if wait:
            delay = (reset - datetime.now()).total_seconds() + buffer
            print "Sleeping for {}s...".format(delay)
            sleep(delay)
            #We have waited for the rate limit reset. OK to proceed.
            return True
        else:
            #We have reached the rate limit. The user needs to handle the rate limit manually.
            return False

    #We have not reached the rate limit
    return True

class StreamListener(tweepy.StreamListener):
    def on_status(self, status):
        #print "From on_status", (status.aext)
        return

    def on_delete(self, status_id, user_id):
        self.delout.write( str(status_id) + "\n")
        return

    def on_limit(self, track):
        sys.stderr.write(track + "\n")
        return

    def on_error(self, status_code):
        sys.stderr.write('Twitter API Error: ' + str(status_code) + "\n")
        if status_code == 420:
            sys.stderr.write('Waiting to reconnect\n')
            time.sleep(60)
            sys.stderr.write('Reconnecting\n')
        if status_code == 401:
            os.system('sudo ntpdate ntp.org')
        return False

    def on_timeout(self):
        sys.stderr.write("Timeout, sleeping for 60 seconds...\n")
        time.sleep(60)
        return
    def on_data(self, data):
        try:
           tweet = json.loads(data)
           #if track_query in tweet['text'] and tweet['entities']:
           create_events(tweet)
           return True

        except BaseException, e:
            print "Hit Base Exception ", str(e)
            time.sleep(5)

auth = tweepy.OAuthHandler(config['consumer_key'], config['consumer_secret'])
auth.set_access_token(config['access_token'], config['access_token_secret'])
api = tweepy.API(auth)

client = pymongo.MongoClient('localhost', 27017)
database = client[stream_name]
#print database
collection = database["tweets"]
#for r in collection.find():
#     print r
thread.start_new_thread(day_caching.timed_days, (database, stream_name, config['caching_frequency']))

if __name__ == '__main__':
     while True:
      try:
          stream_listener = StreamListener()
          stream = tweepy.Stream(auth=api.auth, listener=stream_listener)
          stream.filter(track=["#" + stream_name], stall_warnings=True)
      except AttributeError as ae:
          if "NoneType" or "ReadTimeoutError" in ae:
              pass
          else:
              print "error occurred:" + str(ae)
      except:
          continue
