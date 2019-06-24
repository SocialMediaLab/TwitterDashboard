import datetime, time
import daily

def timed_days (database, hashtag, frequency):
	while True:
		now = datetime.datetime.now()
		daily.make_day(database, now, hashtag)
		print "Cached Today's Data"
		time.sleep(frequency)
	print "exited loop"