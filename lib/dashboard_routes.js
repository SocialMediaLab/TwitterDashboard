var util = require('util');
var mongoose = require('mongoose')
var url = require('url')
var express = require('express')

module.exports = function (app, collection) {
	let start, end

	app.get('/' + hashtag, function (req, res) {
		var dataHolder;
		mongoose.connect('mongodb://localhost/' + hashtag, function(err, db) {
			
			dayCollection = db.collection('days');
			tweetCollection = db.collection('tweets');

			var start = parseInt(req.query.startDay)
			if (isNaN(start)) {start = 8};
			var end = parseInt(req.query.endDay)
			if (isNaN(end)) {end = 1};

			var query = getQuery(start, end)

			var dateString = query.$or[start - end - 1]._id  + ' - ' +  query.$or[end]._id

			dayCollection.find(query).toArray(function (err, days) {
				tweetCollection.find({'_id':'QUEUE'}).toArray(function(err, recentTweets) {
					res.render(
						'index',
						{
							days: days,
							tweetQueue: recentTweets,
							dates: dateString,
							hashtag: hashtag
						}	
					);
					console.log('Rendered ' + hashtag + " " + dateString)
				})
			})
		})
	})

	function getQuery(start, end) {

		const monthNames = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
		var query = new Array(start - end)

		for (var i = end; i < start; i++) {
			var day = new Date();
			day.setDate(day.getDate() - i)
			var n = "0" + day.getDate()
			n = n.substr(n.length - 2)
			var docName = monthNames[day.getMonth()] + ' ' + n
			//console.log(docName)
			query[i - end] = {'_id': docName}
			//console.log(i)
		}
		return({'$or': query})
	}
}