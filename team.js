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

module.exports.crawlTeam = function(url){
	let crawler = new Crawler().configure({
		ignoreRelative: true,
		dept: 1,
		maxRequestsPerSecond: 100,
		maxConcurrentRequests: 10,
		forgetCrawled: true,
		shouldCrawl: function (url1){
			return url1.indexOf("sports.yahoo.com/nfl/teams") > 0;
		}
	});
	crawler.crawl({
		url: url,
		success: function (page) {
			let urlTail = module.exports.getUrlTail(page.url, '/', 2);
			let $ = cheerio.load(page.content);
			let url = new URL(page.url);
			let baseUrl = url.protocol  +  "//"  +  url.hostname;
			if (urlTail.trim() == 'teams') {      
				let teamLinks = $("a[href^='/nfl/teams/']");
				let crawlUrls = [];
				teamLinks.each(function () {
					let href = $(this).attr('href');
					let roster = module.exports.getUrlTail($(this).attr('href'),'/',2);

					roster = roster.trim();
					if (roster == 'roster') {
						roster = null;
						page = null;
						let urlObj = baseUrl + href;
						crawlUrls.push(urlObj);
					}
				}
				);
				async.eachSeries(crawlUrls, module.exports.crawlPlayerTeam, function() {
					console.log('Insert player_pos, teams_players done');
					console.log(crawlUrls);
				});    
			}
			else { 
				let name = $('.ys-name').text();
				if (name)
				{
					let team_id = module.exports.getUrlTail(page.url, '/', 2).toUpperCase();
					let team ={name: name, team_id: team_id};
					if (name != ''){
						let sql = "INSERT INTO teams(code,team_name,last_changed) values(?,?,NOW()) ON DUPLICATE KEY UPDATE "
						+"team_name=values(team_name), last_changed=NOW()";
						let inserts  = [team.team_id,team.name];
						sql = mysql.format(sql, inserts);

						pool.getConnection(function(err, connection) {
							if (err) {
								console.log(err);
								return ;
							}
							connection.query(sql, function(err, rows) {
								connection.destroy();
								if (err) console.log('Error running query', err);
								else if(rows.insertId != null) console.log('Inserted team id ', rows.insertId);
								else console.log('Update team', team.code);
							});

						});
					}


				}
			}
		},
		failure: function (page) {
			console.log('Fail to load page '  +  page.url  +  ' -- '  +  page.status);
		},
		finished: function (crawledUrls) {
			console.log('Finished crawling teams ', crawledUrls.length);
		}
	});
	
}

module.exports.crawlPlayerTeam = function(url, callback) {
	let crawlerPlayerByTeam = new Crawler().configure({
		ignoreRelative: true,
		dept: 1,
		maxRequestsPerSecond: 100,
		maxConcurrentRequests: 10,
		forgetCrawled: true,
		shouldCrawl: function (url) {
			return url.indexOf("teams") > 0 && url.indexOf("roster") > 0;
		}
	});

	crawlerPlayerByTeam.crawl({
		url: url,
		success: function (page) {
			let $ = cheerio.load(page.content);
			let playerLinks = $("a[href^='/nfl/players/']");
			let team_id = module.exports.getUrlTail(page.url, '/', 3).toUpperCase();
			let queries = '';
			let insertTeamPlayer = '';
			async.waterfall([
				function(callback) {
					playerLinks.each(function () {
						let id =module.exports.getUrlTail($(this).attr('href'), '/', 2);
						if (id > 0) {
							let name = $(this).text().trim();
							let tr = $(this).parent().parent().parent().parent();
							let num = tr.children('td').eq(0).text().trim();
							let spanPos = tr.children('td').eq(2).children().children();
							let pos = [];
							spanPos.each(function(){
								pos.push($(this).text().replace('/',''));
							});


							insertTeamPlayer += "insert into teams_players(player_id,team_id,last_changed) values(" + id + "," +
							pool.escape(team_id) + ",NOW()) ON DUPLICATE KEY UPDATE " +
							"last_changed=NOW();";

							if ( parseInt(num) > 0) {
								name=tr=num=spanPos=null;

								pos.forEach(function(item){
									queries += "INSERT INTO player_pos(team_id,player_id,pos,last_changed) values(" +
									pool.escape(team_id) + "," + pool.escape(id) + "," + pool.escape(item)
									+ ",NOW()) ON DUPLICATE KEY UPDATE last_changed=NOW();";
								});

							}
						}
					}); 
					let sqls = insertTeamPlayer + queries;
					if (sqls) callback(null, sqls); else callback(true);
				},
				function(sqls, callback) {
					pool.getConnection(function(err, connection) {
						if (err) {
							console.log(err);
							throw err;
						}
						connection.query(sqls, function(err, rows) {
							connection.destroy();
							if (err) console.log('Error running query', err);
							console.log('Insert/Update teams_players/player_pos');
						});
						callback();
					});
				}
				], function() { callback(); });
			
		},
		failure: function (page) {
			console.log('Fail to load page '  +  page.url  +  ' -- '  +  page.status);

		},
		finished: function (crawledUrls) {
			console.log('Finished team crawling ', crawledUrls);
		}
	});
}