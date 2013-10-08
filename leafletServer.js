var Datastore = require('nedb');
var db = new Datastore({ filename: 'nedbfile.txt' });
var http = require('http');
var connect = require('connect');
var qs = require('querystring');
var url = require('url');
var fs = require('fs');
var path = require('path');

var async = require('async');
var jsR = {};

var AWS = require('aws-sdk');
// Instead, do this:
AWS.config.loadFromPath('credentials.json');

// Set your region for future requests.
AWS.config.update({region: 'us-east-1'});

var dynamodb = new AWS.DynamoDB();

async.series([
    function(callback){
        db.ensureIndex({ fieldName: 'lat' }, function (err) {
            // If there was an error, err is not null
            callback(err);
        });
    },
    function(callback){
        db.ensureIndex({ fieldName: 'lon' }, function (err) {
            // If there was an error, err is not null
            callback(err);
        });
    },
    function(callback){
        db.loadDatabase(function (err) {
            callback(err);

        });
    }
],
// optional callback
function(err, results){
    // results is now equal to ['one', 'two']
    if(err)
    {
        console.log("neDB Setup Error");
    }
    else
    {
        connect()
        .use(connect.favicon())
        .use(connect.cookieParser())
        .use(connect.session({ secret: 'secret', cookie: { maxAge: 600000000 }}))
        .use(connect.static(__dirname + '/public/', { maxAge: 86400000 }))
        .use(function(req, res, next){
            var sess = req.session;
            //ROUTING
            if (req.url == '/logout')
            {
                sess.destroy();
                res.setHeader('Content-Type', 'text/html');
                res.write('You are logged out.');
                res.end();
            }
            else if (req.url.match(/\/vbgetmapvenues/gi))
            {
                var parsedqs = qs.parse(url.parse(req.url).query);
                var ai = parsedqs.ai;
                var al = parsedqs.al;
                var oi = parsedqs.oi;
                var ol = parsedqs.ol;
                
                console.log(!!ai && !!al && !!oi && !!ol);
                
                if(!!ai && !!al && !!oi && !!ol)
                {
                    db.find({$and: [{lat: {$gte: parseFloat(al)}}, {lat: {$lte: parseFloat(ai)}}, {lon: {$lte: parseFloat(oi)}}, {lon: {$gte: parseFloat(ol)}}]}, function (err, docs) {
                        //map docs to BatchGet request
                        if (docs.length > 0)
                        {
                            var bgkeys = docs.map(
                                function(e, i, arr) {
                                    return {"id": {"S": e.id}}
                                }
                            );
                            var batchgetreqparams = {
                                "RequestItems": {
                                    "vb_main": {
                                        "Keys": bgkeys,
                                        "AttributesToGet": [
                                            "doc", "id"
                                        ]
                                    }
                                }
                            }
                            
                            dynamodb.batchGetItem(batchgetreqparams, function (err, data) {
                                if (err) {
                                    console.log('dynamoDB batchGet Error catch: ' + err); // an error occurred
                                    res.setHeader('Content-Type', 'application/json');
                                    res.setHeader('Charset', 'utf-8');
                                    res.write(parsedqs.callback + '(' + JSON.stringify([]) + ');');     
                                    res.end();
                                } 
                                else if(data) {
                                    res.setHeader('Content-Type', 'application/json');
                                    res.setHeader('Charset', 'utf-8');
                                    //res.write(parsedqs.callback + '({"something": "rather", "more": "pork", "tua": "tara"});');
                                    res.write(parsedqs.callback + '(' + JSON.stringify(data.Responses.vb_main.map(
                                        function(e, i, arr) {
                                            return JSON.parse(e.doc.S);
                                        }
                                    )) + ');');                 
                                    res.end();
                                }
                                else {
                                    console.log('dynamoDB BatchGet Error else: ' + err); // an error occurred
                                    res.setHeader('Content-Type', 'application/json');
                                    res.setHeader('Charset', 'utf-8');
                                    res.write(parsedqs.callback + '(' + JSON.stringify([]) + ');');     
                                    res.end();
                                }
                            });
                        }
                        else 
                        {
                            console.log('empty req: '); // an error occurred
                            res.setHeader('Content-Type', 'application/json');
                            res.setHeader('Charset', 'utf-8');
                            res.write(parsedqs.callback + '(' + JSON.stringify([]) + ');');     
                            res.end();
                        }
                        
                        //console.dir(batchgetreqparams);
                    });
                }
                else
                {
                    //write []
                    res.setHeader('Content-Type', 'text/html');
                    res.end();
                }
                
            }
            else
            {
                res.setHeader('Content-Type', 'text/html');
                res.write('You are logged out.');
                res.end();
            }
        }).listen(3000);
        
        var io = require('socket.io').listen(3001);
        
        io.sockets.on('connection', function (socket) {
          socket.on('venreq', function (data) {
            var ai = data.lathi;
            var al = data.latlo;
            var oi = data.lonhi;
            var ol = data.lonlo;
            
            console.log('pass:\n'); 
            console.log(!!ai && !!al && !!oi && !!ol);
            
            console.dir(socket.store);
            
            if(!!ai && !!al && !!oi && !!ol)
            {
            
                db.find({$and: [{lat: {$gte: parseFloat(al)}}, {lat: {$lte: parseFloat(ai)}}, {lon: {$lte: parseFloat(oi)}}, {lon: {$gte: parseFloat(ol)}}]}, function (err, docs) {
                    //map docs to BatchGet request
                    if (docs.length > 0)
                    {
                        //CASE: query returns some documents (docs)
                        
                        //bgkeys: array of ids in query that have not already been sent to socket 
                        var bgkeys = docs.map(
                            function(e, i, arr) {
                                if(!socket.store.data[e.id])
                                {
                                    //CASE: The id is not present on the socket, so it should be emitted to client
                                    //console.log('eid not found on socket');
                                    //DO: return the dynamoDB-formed request object back to the map function
                                    return {"id": {"S": e.id}};
                                }
                                else
                                {
                                    //CASE: The doc for this id was already sent over this socket...no need to send it again
                                    console.log('eid found on socket');
                                }
                            }
                        );
                        
                        //Prepare the AWS dynamoDB Request object
                        var batchgetreqparams = {
                            "RequestItems": {
                                "vb_main": {
                                    "Keys": bgkeys,
                                    "AttributesToGet": [
                                        "doc", "id"
                                    ]
                                }
                            }
                        };
                        //DO: Send request to AWS dynamoDB 
                        dynamodb.batchGetItem(batchgetreqparams, function (err, data) {
                            if (err) {
                                //CASE: dynamoDB request yields error
                                //DO: log error
                                console.log(err);
                            } 
                            else if(data) {
                                //CASE: dynamoDB request yields data
                                //DO: I.Emit the document to the socket & II.Set the doc.id to true on the socket store so it doesn't get set again.
                                data.Responses.vb_main.map(
                                    function(e, i, arr) {
                                        //DO: II.Set the doc.id to true on the socket store so it doesn't get set again.
                                        socket.set(e.id.S, true, function(){
                                            
                                        });
                                        return JSON.parse(e.doc.S);
                                    }
                                );
                                
                            }
                            else {
                                
                            }
                        });
                    }
                    else 
                    {
                        //CASE: query returns no documents
                        //NoOp
                    }
                    //console.dir(batchgetreqparams);
                });
            
            
            }
            
            console.log('socketvenreq:\n');
            console.log(data);
          });
        });
    
    }
});
