var express = require('express');
var app = express();
var http = require('http'),
    https = require('https'),
    querystring = require('querystring'),
    Step = require('step'),
    fs = require('fs'),
    Gnip = require('gnip');
    // jQuery = require('jquery');
    // parseString = require('xml2js').parseString;


var config = {}
try {
    var config = require('./config')
} catch(e) { }




function sql_escape (str) {
    return str.replace(/[\0\x08\x09\x1a\n\r"'\\\%]/g, function (char) {
        switch (char) {
            case "\0":
                return "\\0";
            case "\x08":
                return "\\b";
            case "\x09":
                return "\\t";
            case "\x1a":
                return "\\z";
            case "\n":
                return "\\n";
            case "\r":
                return "\\r";
            case "'":
                return "''";
            case "\"":
            case "\\":
            case "%":
                return "\\"+char; // prepends a backslash to backslash, percent,
                                  // and double/single quotes
        }
    });
}

String.prototype.format = (function (i, safe, arg) {
    function format() {
        var str = this,
            len = arguments.length + 1;

        for (i = 0; i < len; arg = arguments[i++]) {
            safe = typeof arg === 'object' ? JSON.stringify(arg) : arg;
            str = str.replace(RegExp('\\{' + (i - 1) + '\\}', 'g'), safe);
        }
        return str;
    }

    //format.native = String.prototype.format;
    return format;
})();


function callCartoDB(sql) {
    var post_data = querystring.stringify({
          'api_key' : config.cartodb.api_key,
          'q': sql
    });
    var post_options = {
            host: config.cartodb.username+'.cartodb.com',
            port: '80',
            path: '/api/v2/sql',
            method: 'POST',
            headers: {
                'Content-Type': 'application/x-www-form-urlencoded',
                'Content-Length': post_data.length
            }
    };  
    var post_req = http.request(post_options, function(res) {
          res.setEncoding('utf8');
          res.on('data', function (chunk) {
              process.stdout.write(".");
          });
      });
    // console.log(post_data)
    post_req.write(post_data);
    post_req.end();
}



function toSqlTime(val){
  val = val.trim();
  val =  val == '' ? null : "'{0}'::timestamp".format(sql_escape(val));
  return val 
}
function toSqlString(val){
  val = val.trim();
  val =  val == '' ? null : "'{0}'".format(sql_escape(val));
  return val 
}
function get_point(val){
  var point = null;
  if ("geo" in val){
    if ("coordinates" in val.geo){
      return 'CDB_LatLng({0}, {1})'.format(val.geo.coordinates[0], val.geo.coordinates[1]);
    }
  }
  return point;
}

function tweet2sql(tweet){
  // console.log(tweet.gnip.matching_rules)
  rules = [];
  for (i in tweet.gnip.matching_rules){
    rules.push(tweet.gnip.matching_rules[i].value)
  }
  var curr_sql = 
    '({0}, {1}, {2}, {3}, {4}, {5}, {6})'.format(
      toSqlString(tweet.id),
      toSqlString(tweet.actor.id),
      toSqlString(tweet.actor.preferredUsername),
      toSqlString(tweet.body),
      toSqlTime(tweet.postedTime),
      get_point(tweet),
      toSqlString(rules.join(','))
    );
  return curr_sql;
};

var sql = []
var queue_limit = 125;
function queue(entry){
  sql.push(entry);
  if (sql.length > queue_limit){
    insert(sql);
    sql = [];
  }
}

var base_sql = "WITH n(id, uid, username, body, postedtime, the_geom, rules) AS (VALUES {0}) INSERT INTO world_cup_live (id, uid, username, body, postedtime, the_geom, rules) SELECT n.id, n.uid, n.username, n.body, n.postedtime, n.the_geom, n.rules FROM n WHERE n.id NOT IN (SELECT id FROM world_cup_live)";
var total_inserts = 0;

function insert(inserts){
  callCartoDB(base_sql.format(inserts.join()));
  total_inserts += queue_limit;
}

var run = function(){
  var stream = new Gnip.Stream({
      url : 'https://stream.gnip.com:443/accounts/'+config.gnip.account+'/publishers/twitter/streams/track/prod.json',
      user : config.gnip.username,
      password : config.gnip.password
  });
  // console.log(stream)
  stream.on('ready', function() {
      console.log('Stream ready!');
  });
  stream.on('tweet', function(tweet) {
      var data = tweet2sql(tweet);
      queue(data);
      // console.log(data);
  });
  stream.on('error', function(err) {
      console.error(err);
  });
  stream.start();
}


app.set('port', (process.env.PORT || 5000))
app.use(express.static(__dirname + '/public'))

app.get('/', function(request, response) {
  response.send('Your app has made {0} total inserts</br>There are {1} inserts queued'.format(total_inserts, sql.length))
})

app.listen(app.get('port'), function() {
  console.log("Node app is running at localhost:" + app.get('port'));
  run();
})

