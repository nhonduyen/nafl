"use strict";
var moment = require('moment');
var async  = require('async');
var team   = require('./team');
var player = require('./player');

async.series([
	function(callback) {
		team.crawlTeam('http://sports.yahoo.com/nfl/teams', callback);
		//callback();
	},
	function(callback) {
		player.crawlPlayer(callback);
	},
	function(callback) {
		player.normalizeStats(callback);
		//callback();
	}
	], function done(err) {
	console.log('All done', moment().format('YYYY-MM-DD HH:mm:ss'));
});


