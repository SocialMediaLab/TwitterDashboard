#!/usr/bin/python3.4

#create_stream.py
#Requires: Access keys for Twitter API and Keen.io project
#Usage: % python create_stream.py <twitter query>


from __future__ import unicode_literals
import sys
import nltk
from nltk.corpus import stopwords
from nltk import word_tokenize, pos_tag, ne_chunk
from nltk.tree import Tree
import os
import keen
import tweepy
import time
import json
from textblob import TextBlob
import argparse
import cgi
import re
import ConfigParser as configparser
from nltk.tag import StanfordNERTagger
from nltk.tokenize import word_tokenize



#use program arguments as twitter query
parser = argparse.ArgumentParser(description='Provide twitter query: keywords, @user_handle, #hashtags')
parser.add_argument('-q', '--query', type=str, default=[], nargs='+', help='provide a query for twitter')
parser.add_argument('-c', '--config', dest='config',type=argparse.FileType(mode='r'))
args = parser.parse_args()
query =[]
for arg in args.query:
	arg = "#%s"%arg
        query.append(str(arg))	

print str(query[0]);

"""
here = os.path.realpath('../config/dashboard.config')

if args.config:
   config_file=args.config
   config = configparser.ConfigParser(defaults = {'here': here})
   config.read(args.config)
"""
#extract all environment variables
access_token = os.environ.get('ACCESS_TOKEN')
access_token_secret = os.environ.get('ACCESS_TOKEN_SECRET')
consumer_key = os.environ.get('CONSUMER_KEY')
consumer_secret = os.environ.get('CONSUMER_SECRET')
keen.project_id = os.environ.get('KEEN_PROJECT_ID')
keen.write_key = os.environ.get('KEEN_WRITE_KEY')
keen.read_key = os.environ.get('KEEN_READ_KEY')


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

     
     keen.add_event(str(query[0][1:]),{
                "ID":tweet['id_str'],
                "text":tweet['text'].encode('ascii','ignore'),
                "username":tweet['user']['screen_name'],
                "hashtag1":hashtag1,
                "hashtag2":hashtag2,
                "created_at":tweet['created_at'],
                "mentions1":mentions1,
                "mentions2":mentions2,
                "urls1":urls1,
                "urls2":urls2,
                "sentiment_range":sentiment_range,
                "sentiment":sentiment,
                "subjectivity":subjectivity,
                "subjectivity_range":subjectivity_range,
                "named_entities":named_entities,
                "retweeted":retweet_status,
                "replied_to":tweet['in_reply_to_screen_name'],
                "media":media
       })


class StreamListener(tweepy.StreamListener):
    def on_status(self, status):
        #print "From on_status", (status.text)
        return

    def on_delete(self, status_id, user_id):
        self.delout.write( str(status_id) + "\n")
        return

    def on_limit(self, track):
        sys.stderr.write(track + "\n")
        return

    def on_error(self, status_code):
        sys.stderr.write('Error: ' + str(status_code) + "\n")
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

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api = tweepy.API(auth)


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
    remaining = int(api.last_response.getheader('x-rate-limit-remaining'))
    if remaining == 0:
        limit = int(api.last_response.getheader('x-rate-limit-limit'))
        reset = int(api.last_response.getheader('x-rate-limit-reset'))
        reset = datetime.fromtimestamp(reset)
        print "0 of {} requests remaining until {}.".format(limit, reset)

        if wait:
            delay = (reset - datetime.now()).total_seconds() + buffer
            print "Sleeping for {}s...".format(delay)
            sleep(delay)
            return True
        else:
            return False

    return True

if __name__ == '__main__':
     if len(sys.argv)==0:
	print "Please provide a hashtag to establish a query"
     while True:
      try:
          stream_listener = StreamListener()
          stream = tweepy.Stream(auth=api.auth, listener=stream_listener)
	  stream.filter(track=[str(query[0])], stall_warnings=True)
      except AttributeError as ae:
          if "NoneType" or "ReadTimeoutError" in ae:
              pass
          else:
              print "error occurred:" + str(ae)
      except:
          continue
