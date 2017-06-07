"use strict";
var async  = require('async');
var team   = require('./team');
var player = require('./player');

//team.crawlTeam('http://sports.yahoo.com/nfl/teams');
player.crawlPlayer();
//player.normalizeStats();

