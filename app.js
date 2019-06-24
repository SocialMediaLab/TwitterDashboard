var express = require('express');
var path = require('path')
var http = require('http').Server(express)
var port = 8999;

// Process Input
if (process.argv.length < 3) {
	console.log("Please enter hashtag in command line with no quotes or symbols: node app.js hashtag")
	process.exit()
} else {
	global.hashtag = process.argv[2]
	console.log("Hashtag: " + hashtag)
}

// Start collection script (Python)
var spawn = require('child_process').spawn('python', 
	['scripts/create_stream.py', hashtag],
	{stdio: [process.stdin, process.stdout, process.stderr]}
)

var app = express.createServer();

app.configure(function () {
	app.use(express.cookieParser());
	app.use(express.session({ secret: 'example' }));
	app.use(express.bodyParser());
	app.use(app.router);
	app.use('/', express.static(path.join(__dirname, 'public')))
	app.set('view engine', 'ejs');
	app.set('view options', { layout: false });

});
require('./lib/dashboard_routes.js')(app);

app.listen(port);
console.log('Node listening on port %s', port);
