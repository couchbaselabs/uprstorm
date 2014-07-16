var app = require('express')();
var http = require('http').Server(app);
var io = require('socket.io')(http);
var couchbase = require('couchbase');
var topics = {};

var db = new couchbase.Connection({
  bucket:"default",
  password:"",
  host:"localhost:8091"
});

app.get('/', function(req, res){
  res.sendfile('index.html');
});

app.get('/css/:file', function(req, res){
  var file = req.params.file;
  res.sendfile('css/'+file);
});



app.get('/storm/:topic/:rank/:count/:id', function(req, res){
      var topic = '#'+req.params.topic;
      var rank = req.params.rank;
      var count = req.params.count;
      var id = req.params.id;
      var txt = '';
      var cls = 'none';

      if (topic in topics){
        var oldRank = topics[topic];
        /*if (rank < oldRank){ 
            cls = 'success';
        }
        if (rank > oldRank){
            cls = 'error';
        } */
      } else {
        cls = 'success';
      }

      topics[topic] = rank;

      db.get(id, function(err, result) {
          if (err){
            msg = 'key missing: '+id
            console.log(msg);
            txt = '';
          } else {
            txt = result.value['text'];
          }
          //console.log(topics);
          //console.log(cls);
          io.emit('message', {'rank': rank,
                              'topic': topic,
                              'count': count,
                              'txt': txt,
                              'cls' : cls});
         res.send('ok');
      });

});

app.get('/spout/:vb/:last/:high', function(req, res){
      var vb = req.params.vb;
      var last = req.params.last;
      var high = req.params.high;
      io.emit('vbucket', {'vb': vb, 'last': last, 'high':high});
      res.send('ok');

});
io.on('connection', function(socket){


  console.log('a user connected');

  socket.on('rankings', function(rankings){
    // purge topics hash
    for (topic in topics){
        if (rankings.indexOf(topic) < 0){
            delete topics[topic];
        }
    }
  });

});


http.listen(3000, function(){
  console.log('listening on *:3000');
});
