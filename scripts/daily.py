import pymongo
import datetime
from datetime import datetime

def make_day(database, date, hashtag):
	day_collection = database["days"]
	tweet_collection = database["tweets"]
	entity_collection = database["entities"]

	today = date.strftime('%b %d')
	date_filter = {
		"created_at" :
			{'$regex': today}
		}
	#print "All-time total: ", tweet_collection.find({}).count()

	tweets = tweet_collection.find(date_filter)
	total = tweets.count()
	#print "Total: 	", total 
	
	positive = tweet_collection.find(
			{'$and':
				[{"sentiment_range": "positive"},
				date_filter]
			}
		).count()
	#print "Positive: ", positive

	negative = tweet_collection.find(
			{'$and':
				[{"sentiment_range": "negative"},
				date_filter]
			}
		).count()
	#print "Negative: ", negative

	subjective = tweet_collection.find(
			{'$and':
				[{"subjectivity_range": "subjective"},
				date_filter]
			}
		).count()
	#print "Subjective: ", subjective

	not_subjective = total - subjective
	#print "Not Subjective: ", not_subjective

	mentions = total - tweet_collection.find(
			{'$and':
				[{"mentions1": "None"},
				date_filter]
			}
		).count()
	#print "Mentions: ", mentions

	hashtags = total - tweet_collection.find(
			{'$and':
				[{'$or':  [
					{"hashtag2": "cdnpoli"},
					{"hashtag2": "CdnPoli"},
					{"hashtag2": "CDNPoli"},
					{"hashtag2": "CDNPOLI"},
					{"hashtag2": "None"}
				]},
				date_filter]
			}
		).count()
	#print "Hashtags: ", hashtags

	urls = total - tweet_collection.find(
			{'$and':
				[{'urls1': 'None'},
				date_filter]
			}
		).count()
	#print "URLs: ", urls

	media = total - tweet_collection.find(
			{'$and':
				[{'media': 'false'},
				date_filter]
			}
		).count()
	#print "Media: ", media

	hours = []
	for i in range(0, 24):
		hours.append(tweet_collection.find(
			{'$and':
				[{"created_at": {'$regex': ' ' + str(i).zfill(2) + ':'}},
				date_filter]
			}
		).count())
	#print "Hours: ", hours

	top_hashtags = getTop10('hashtag2', date_filter, tweet_collection, hashtag)

	top_entities = getTop10('named_entities', date_filter, tweet_collection, hashtag)

	top_contributors = getTop10('username', date_filter, tweet_collection, hashtag)

	top_mentioned = getTop10('mentions1', date_filter, tweet_collection, hashtag)

	day = {
		'_id': today,
		'total': total,
		'positive': positive,
		'negative': negative,
		'subjective': subjective,
		'not_subjective': not_subjective,
		'mentions': mentions,
		'hashtags': hashtags,
		'urls': urls,
		'media': media,
		'hours': hours,
		'top_hashtags': top_hashtags,
		'top_entities': top_entities,
		'top_contributors': top_contributors,
		'top_mentioned': top_mentioned
	}

	try:
		day_collection.insert_one(day)
	except:
		day_collection.replace_one( {'_id': today}, day )



def getTop10(field_name, date_filter, collection, hashtag) :
	return list(collection.aggregate([
			{'$match': date_filter},
			{'$match': {field_name : {'$ne': 'None'}}},
			{'$match': {field_name : {'$ne': []}}},
			{'$match': {field_name : {'$ne': hashtag}}},
			{'$sortByCount': '$' + field_name},
			{'$limit': 10}
		]))
