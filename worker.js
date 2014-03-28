var amqp = require('amqp');
var path = require('path');
var fs = require('graceful-fs');
var oboe = require('oboe');
var q = require('q');

var jsonDir = path.join(__dirname,'data/RI/');
var id = "4378083";

var connection = amqp.createConnection({host: 'localhost'});

connection.on('ready', function(){
    connection.queue('task_queue', {autoDelete: false,
                                    durable: true}, function(queue){

        console.log(' [*] Waiting for messages. To exit press CTRL+C');

        queue.subscribe({ack: true, prefetchCount: 1}, function(msg){
            var body = msg.data.toString('utf-8');
            console.log(" [x] Received %s", body);
            findVals(body, id);
            setTimeout(function(){
                console.log(" [x] Done");
                queue.shift(); // basic_ack equivalent
            }, (body.split('.').length - 1) * 10);
        });
    });
});


function findVals(file, id) {
  var deferred = q.defer();
  // console.log("function one");
  console.log(file);
  console.time("findVals");
  oboe(fs.createReadStream(jsonDir + file, {
    autoClose: true
  })).on('node',
      {
        'selections': function(scheme){
           //console.log(scheme);
         },
        '{nodeId type}': function(item, path, ancestors) {
          if (item.type === 'GROUP_LEVEL_ITEM' && item.nodeId === id) {
              console.log(item);
              console.log('FILE: ' + file);
              console.dir(path);
              console.timeEnd("findVals");

              // console.log('ANCESTORS_0: ' + ancestors[0]);
              // console.dir('ANCESTORS: ' + ancestors);
            }
        }
      // }).on('done', function (json) {
      //   console.log('request completed');
      //   console.timeEnd("findVals");

      });
      deferred.resolve();


  return deferred.promise;
}
