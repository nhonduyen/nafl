"use strict";
var moment = require('moment');
var mysql = require('mysql');
var request = require('request');
var cheerio = require('cheerio');
var URL = require('url-parse');
var Crawler = require("js-crawler");
var async = require('async');

let pool  = mysql.createPool({
	connectionLimit : 20,
	acquireTimeout  : 5 * 60 * 1000,
	connectTimeout  : 5 * 60 * 1000,
	timeout         : 5 * 60 * 1000,
	host        	: 'localhost',
	user        	: 'root',
	password    	: '',
	database    	: 'sport',
	multipleStatements: true
});


module.exports.getUrlTail = function(url, sym, index){
	let arrUrlParam = url.split(sym);
	return arrUrlParam[arrUrlParam.length - index];
}

module.exports.crawlPlayer = function() {
	
	let alphabet = "abcdefghijklmnopqrstuvwxyz".split("");
	let crawlUrls = [];
	for (let i = alphabet.length - 1; i >= 0; i--) {
		let searchLink = "https://sports.yahoo.com/site/api/resource/sports.league.playerssearch;count=1000;league=nfl;name="+alphabet[i]+";pos=;"+
		"start=?bkt=[%22sp-rz-aa-001%22,%22topcomment2-test5%22,%22sp-strm-lge-test3%22,%22sp-ssl-test%22,%22spdmtest%22,%22sp-s2-bball-all%22,"+
		"%22sp-q2-reel-aa-2%22,%22spexp1-AA-5pct01%22,%22sp-full-ssl-control%22,%22sp-ssl2-test%22,%22sp-ssl3-control%22,%22sp-ss4-test%22,"+
		"%22sp-ssl5-test%22,%22sp-ssl6-test%22,%22sp-football-landing-ctl%22]&device=desktop&feature=canvassOffnet,newContentAttribution,"+
		"livecoverage,canvass,forceDarlaSSL,s2dedup&intl=us&lang=en-US&partner=none&prid=5351df9cgnl6j&region=US&site=sports&tz=America/"+
		"Los_Angeles&ver=1.0.1412&returnMeta=true";
		crawlUrls.push(searchLink);
	}
	async.eachSeries(crawlUrls, module.exports.requestPlayer, function() {
		console.log('Insert players/stats done');
	});    
	
}

module.exports.requestPlayer = function(url, callback) {
	async.waterfall([
		function(callback) {
			request({
				url: url,
				json: true
			}, function (error, response, body) {
				if (!error && response.statusCode === 200) {
					let url1 = new URL(url);
					let baseUrl = url1.protocol  +  "//"  +  url1.hostname;
					if (body.data && body.data.players) {
						for (let data in body.data.players){
							let link = baseUrl + body.data.players[data].home_url + '/gamelog';
							if (/\d/g.test(link)) callback(null, link);							
						}
					}
				}
				else{
					console.log('Error', error);
					callback(true);
				}
			});
		},
		function (url, callback) {
			module.exports.insertPlayerBySearchApi(url, callback);
		},
		function(players, urls, callback) {
			module.exports.insertPlayer(players, urls, callback);
		},
		function(urls, callback) {
			module.exports.insertStat(urls, callback);
		}
		]
		, function(){ callback(); });
	
}
module.exports.insertPlayerBySearchApi = function(url, callback) {
	let categories = ['Passing', 'Rushing', 'Receiving', 'Kicking', 'Returns', 'Punting', 'Defense'];
	let crawler = new Crawler().configure({
		ignoreRelative: true,
		dept: 1,
		maxRequestsPerSecond: 100,
		maxConcurrentRequests: 10,
		forgetCrawled: false,
		shouldCrawl: function (url) {
			return url.indexOf("gamelog") > 0;
		}
	});
	crawler.crawl({ url: url
		, success: function (page) {
			let $ = cheerio.load(page.content);
			let years = $("option","select");
			let name = $('h1').text();
			let team_id = '';
			let pos = '';
			let num = '';

			let teamLinks = $("a[href^='/nfl/teams/']");
			teamLinks.each(function () {
				let temp = module.exports.getUrlTail($(this).attr('href'), '/', 1);
				if (temp && temp != 'teams'){
					team_id = temp;              	 
				}
			});
			let salary = 0;
			let last_season = 0;
			let rookie_season = 0;
			let birth_country ='';
			let exp = 0;
			let Born = null;
			let College = $("span:contains('College')").next().text().trim();
			let height = $("span:contains('Height')").next().children('span').text().trim();
			let weight = $("span:contains('Weight')").next().children('span').text().trim();
			let birth_location = $("span:contains('Birth Place')").next().text().trim();
			let Draft = $("span:contains('Draft')").next().children('span').text().trim();
			let id = module.exports.getUrlTail(page.url, '/', 3);
			num = module.exports.getUrlTail($('span[data-reactid=16]').text(), '#', 1);
			if (isNaN(num)) num = 0;
			pos = $('span[data-reactid=18]').text().trim();

			pos = module.exports.getUrlTail(pos, ',', 2);
			let script = $("script:contains('/* -- Data -- */')").text();
			let indexBio = script.search('"bio":{') + 7;
			let indexEndBio = script.search('},"display_name');
			let arrBio = script.substring(indexBio, indexEndBio).split(',');
			console.log(arrBio);
			for (let i = arrBio.length - 1; i >= 0; i--) {
				let column = arrBio[i].split(':');
				console.log(column);
				console.log(column[1]);
				if (column[1]) {
					let value = column[1].split('"').join('');
					switch (column[0].split('"').join('')) {
          	case "height":  break; //height = value;
          	case "experience":  exp = (value.length == 0) ? 0 : value; break;
          	case "salary":  salary = (value.length == 0) ? 0 : value; break;
          	case "last_season":  last_season = (value.length == 0) ? 0 : value; break;
          	case "rookie_season":  rookie_season = (value.length == 0) ? 0 : value; break;
          	case "birth_date":  Born = (value.length == 0) ? null : value; break;
          	case "birth_location": break; //birth_location = value;
          	case "birth_country":  birth_country = value; break;
          	case "college": break;    	//College = value;
          	case "weight": break;         	//weight = value;
          	break; }}}

          	let player = { team_id: team_id.toUpperCase(), name: name, Height: height, Weight: weight, dob: Born, birthplace: birth_location, College: College
          		,  Draft: Draft, pos: pos, player_id: id, num: num, salary: salary, rookie_season: rookie_season, last_season: last_season
          		,  birth_country: birth_country, exp: exp, birth_location: birth_location};
          		let urls = [];
          		let mycate = '';
          		for (let i = categories.length - 1; i >= 0; i--) {
          			let cate = $("h3:contains('" + categories[i] + "')").text().trim();
          			if (typeof(cate) != 'undefined' && cate.length > 0) {
          				mycate = cate;
          				if (years.length > 0) {
          					cate = null;
          					years.each(function () {
          						let url = 'https://graphite-secure.sports.yahoo.com/v1/query/shangrila/gameLogFootball' + mycate + '?lang=en-US&region=US' +
          						'&tz=Asia%2FHo_Chi_Minh&ysp_redesign=1&playerId=nfl.p.' + id + '&season=' + $(this).text();
          						urls.push(url);
          					});
          				}
          			}
          		}
          		callback(null, player, urls);
          	},
          	failure: function (page) {
          		console.log('Fail to load page '  +  page.url  +  ' -- '  +  page.status);
          	},
          	finished: function (crawledUrls) {
          		if(crawledUrls.length > 0) console.log('Finished players crawling ',crawledUrls);
          		
          	}
          });
}

module.exports.insertPlayer = function(player, urls , callback) {
	let sql = "INSERT INTO players(id,team_id,name,Height,Weight,dob,birthplace,College,Draft,pos,num,salary,birth_country," 
	+"rookie_season,last_season,exp,last_changed) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,NOW()) "
	+"ON DUPLICATE KEY UPDATE team_id=values(team_id),name=values(name),height="
	+"values(height), weight=values(weight),dob=values(dob),birthplace=values(birthplace)"
	+",College=values(College),Draft=values(Draft),pos=values(pos),num=values(num),salary=values(salary),birth_country="
	+"values(birth_country),rookie_season=values(rookie_season),last_season=values(last_season),exp=values(exp), last_changed=NOW()";

	let inserts  = [player.player_id,player.team_id,player.name,player.Height,player.Weight,player.dob,
	player.birth_location,player.College,player.Draft,player.pos,player.num,player.salary,
	player.birth_country,player.rookie_season,player.last_season,player.exp];
	sql = mysql.format(sql, inserts);
	pool.getConnection(function(err, connection) {
		if (err) {
			console.log(sql);
			throw err; return false;
		}
		connection.query(sql, function(err, rows) {
			connection.destroy();
			if (err) console.log('Error running query', err);
			else if(rows.insertId != null) console.log('Inserted player_id ', player.player_id);
			else console.log('Update player ', player.player_id);
		});
	});
	callback(null, urls);
}

module.exports.insertStat = function(urls, callback) {
	async.eachSeries(urls, function(url, callback) {
		let q = url.indexOf('?');
		let p = url.indexOf('p.');
		let player_id = url.substring(p + 2).split('&');
		player_id = player_id[0];
		let cate = url.substring(75, q);
		async.waterfall([
			function(callback) {
				request({
					url: url,
					json: true
				}, function (error, response, body) {
					if (!error && response.statusCode === 200) {
						if (body.data && body.data.players) {
							body.data.players.forEach(function(data){
								data.playerGameStats.forEach( function(stats){
									if( stats.game != null ) {
										let startTime = "" + stats.game.startTime.replace("T"," ").substring(0,20);
										let teamId = stats.teamId;
										let homeTeamId = (stats.game.homeTeamId == null) ? 'null' : stats.game.homeTeamId; 
										let awayTeamId = stats.game.awayTeamId;
										let homeScore = stats.game.homeScore;
										let awayScore = stats.game.awayScore;
										stats.game.teams.forEach(function(team){
											if (	teamId == team.teamId) teamId = team.abbreviation;
											if (homeTeamId == team.teamId) homeTeamId = team.abbreviation;
											if (awayTeamId == team.teamId) awayTeamId = team.abbreviation;  });
										let statsArr = [];

										stats.stats.forEach(function(sta) {
											let field = {title: sta.statId, value: (sta.value == null) ? 'null' : sta.value }; 
											statsArr.push(field); });
										let statsObject = {startTime: startTime,teamId: teamId, homeTeamId: homeTeamId, player_id, cate: cate,
											awayTeamId: awayTeamId, homeScore: homeScore,awayScore: awayScore, statsArr: statsArr};

											callback(null, statsObject);
										}
									});
							});
						}
					}
					else callback(null, undefined);
				});
			},
			function(statsObject,callback) {
				if (statsObject) {
					let sql = "INSERT INTO games(date,t1,t2,t1score,t2score,homefield,last_changed) values("
					+"?,?,?,?,?,?,NOW()) ON DUPLICATE KEY UPDATE "
					+"t1score=values(t1score),t2score=values(t2score),homefield=values(homefield)"
					+", last_changed=NOW();";
					if (statsObject.startTime) statsObject.startTime = statsObject.startTime.replace("T"," ").substring(0,20);
					let inserts = [statsObject.startTime,statsObject.homeTeamId,statsObject.awayTeamId,
					statsObject.homeScore,statsObject.awayScore,statsObject.homeTeamId];
					sql = mysql.format(sql, inserts);
					pool.getConnection(function(err, connection) {
						if (err) {
							throw err; callback(null, undefined, undefined);
						}
						connection.query(sql, function(err, rows) {
							connection.destroy();
							if (err) console.log('Error running query', err);
							else if (rows.insertId != null) { console.log('Insert game id', rows.insertId);}
							else console.log('Update game');
						});
						callback(null, statsObject);
					});
				}
				else {
					if(callback) callback(null, undefined);
				}
			},
			function(statsObject,callback) {
				if (statsObject) {
					let sql = "select id from games where date=? and t1=? and t2=? limit 1";
					let params  = [statsObject.startTime,statsObject.homeTeamId,statsObject.awayTeamId];
					sql = mysql.format(sql, params);

					pool.getConnection(function(err, connection) {
						if (err) {
							throw err;
						}
						connection.query(sql, function(err, rows) {
							connection.destroy();
							if (err) console.log('Error running query', err + sql);
							else if(rows[0] != null) callback(null,statsObject, rows[0].id);
							else callback(null, statsObject, 0);
						});
					});
				}
				else callback(null, undefined, undefined);
			},
			
			function(statsObject, game_id, callback) {
				if (statsObject && game_id) {
					let sql = "INSERT INTO rosters(team_id,player_id,position,game,last_changed) values("
					+"?,?,?,?,NOW()) ON DUPLICATE KEY UPDATE team_id=values(team_id),player_id=values(player_id),"
					+"position=values(position),game=values(game), last_changed=NOW()";
					let inserts = [statsObject.teamId,statsObject.player_id,statsObject.cate,game_id];
					sql = mysql.format(sql, inserts);
					pool.getConnection(function(err, connection) {
						if (err) {
							throw err; callback(true);
						}
						connection.query(sql, function(err, rows) {
							connection.destroy();
							if (err) console.log('Error running query', err);
							else if (rows.insertId != null) { console.log('Insert rosters id', rows.insertId); callback(null,statsObject, game_id);}
							else { console.log('Update rosters affectedRows', rows.affectedRows); callback(null,statsObject, game_id);}
						});
						if(callback) callback(null, undefined, undefined);
					});
				} 
				if(callback) callback(null, undefined, undefined);
			},
			function(statsObject, game_id, callback) {
				if (statsObject && game_id) {
					let tempInsert = "insert into stats(player_id,GAME_ID,STATS_TYPE,TEAM_ID,";
					let tempUpdate = " ";
					tempInsert += statsObject.statsArr.map(function(item){ return item.title;                           	}).join(',');
					tempUpdate += statsObject.statsArr.map(function(item){ return item.title + "="+ item.title; }).join(',');
					tempInsert += ",last_changed) values(" + statsObject.player_id + "," + game_id+ "," + pool.escape(statsObject.cate) + "," + pool.escape(statsObject.teamId) + ",";                                      	 
					tempInsert += statsObject.statsArr.map(function(item){ return item.value;                           	}).join(',');
					tempInsert += ",NOW()) ON DUPLICATE KEY UPDATE "+ tempUpdate+", last_changed=NOW()";
					pool.getConnection(function(err, connection) {
						if (err) {
							throw err; callback(true);
						}
						connection.query(tempInsert, function(err, rows) {
							connection.destroy();
							if (err) console.log('Error running query', err);
							else if (rows.insertId != null) { console.log('Insert stats id', rows.insertId);}
							else { console.log('Update stats affectedRows', rows.affectedRows);}
						});
						if(callback) callback();
					});
				} 
				if(callback) callback();
			}
			], function() { console.log("Finished insert stats"); callback(); });
}, function(err) { callback();});

}

module.exports.normalizeStats = function() {
	async.series([
		function(callback) {
			let sql = "DROP TABLE IF EXISTS `wide`; create table wide as select * from stats;";
			pool.getConnection(function(err, connection) {
				if (err) {
					console.log(err);
					return ;
				}
				connection.query(sql, function(err, rows) {
					connection.destroy();
					if (err) console.log('Error running query', err);
					console.log('Create table wide');
					callback();
				});
				
			});
		},
		function(callback) {
			let sql ="DROP TABLE IF EXISTS `statscolumns`; create table statscolumns(id int auto_increment primary key,column_name varchar(30)); "
			+"insert into statscolumns(COLUMN_NAME) SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'sport'  AND "
			+"TABLE_NAME = 'stats';";			
			pool.getConnection(function(err, connection) {
				if (err) {
					console.log(err);
					return ;
				}
				connection.query(sql, function(err, rows) {
					connection.destroy();
					if (err) console.log('Error running query', err);
					console.log('Create table statscolumns');
					callback();
				});
				
			});
		}
		
		],function(err) {
			module.exports.insertNormalize();
		});
}

module.exports.insertNormalize = function() {
	async.waterfall([
		function(callback) {
			let sql ="select column_name from statscolumns where column_name not in ('id','player_id','game_id','team_id','STATS_TYPE','last_changed');";			
			pool.getConnection(function(err, connection) {
				if (err) {
					console.log(err);
					return ;
				}
				connection.query(sql, function(err, rows) {
					connection.destroy();
					if (err) console.log('Error running query', err);
					callback(null, rows);
					

				});
				
			});
		},
		function(rows, callback) {
			let total = rows.length;
			let finishedCount = 0;
			for (let i = rows.length - 1; i >= 0; i--) {
				let sql = "insert into thin (player_id, game_id, team_id, STATS_TYPE, name, value) "
				+   "select player_id, game_id, team_id, STATS_TYPE, ??,? from wide;";
				let name = rows[i].column_name;
				let inserts  = [rows[i].column_name,rows[i].column_name];
				sql = mysql.format(sql, inserts);
				pool.getConnection(function(err, connection) {
					if (err) {
						console.log(err);
						return ;
					}
					connection.query(sql, function(err, rows) {
						finishedCount++;
						connection.destroy();
						if (err) console.log('Error running query', err);
						console.log('Normalize column '+name+' total: %d/%d affected rows',finishedCount,total,rows.affectedRows);
					});
				});
			}
			callback();
		}
		], function() { console.log('Finished normalizeStats'); });
	
}