var amqp = require('amqp');
var path = require('path');
var fs = require('graceful-fs');
var oboe = require('oboe');
var q = require('q');
var couchbase = require('couchbase');
var JSON = require('JSON');
var _ = require('underscore');

// where is data
var jsonDir = path.join(__dirname,'data/RI/');

// Load up the delta file into map of objects for checking later
var deltaMap = [];
console.time('delta');
var _deltaJson = require('./data/NACNLSPI_20090501_500.json');
// oboe('./data/NACNLSPI_20090501_500.json').node('deltas.*', function(item) {
			// if (item.operation.toLowerCase() === 'update') {
_.each(_deltaJson.deltas, function(d) {
	var changeItem = new Object(); 
	changeItem['dim'] = item.dimension;
	changeItem['nodeType'] = item.type;
	changeItem['changedType'] = item.changedType;
	changeItem['newId'] = item.newId;
	changeItem['oldId'] = item.oldId;
	changeItem['newValue'] = item.newValue;
	changeItem['oldValue'] = item.oldValue;
	changeItem['operation'] = item.operation;
	deltaMap.unshit(changeItem);
	console.log(changeItem);
})


console.timeEnd('delta');
console.log('delta');
console.log(deltaMap);
console.log('/delta');

_.each(deltaMap, function(ch) {
	console.log(ch);
  
});


// read from queue for items
var connection = amqp.createConnection({host: 'localhost'});

connection.on('ready', function(){
    connection.queue('task_queue', {autoDelete: false,
                                    durable: true}, function(queue){

        console.log(' [*] Waiting for messages. To exit press CTRL+C');

        queue.subscribe({ack: true, prefetchCount: 1}, function(msg){
            var body = msg.data.toString('utf-8');
            console.log(" [x] Received %s", body);
            findVals(body);
            setTimeout(function(){
                console.log(" [x] Done");
                queue.shift(); // basic_ack equivalent
            }, (body.split('.').length - 1) * 500);
        });
    });
});

// search the RI for changes
function findVals(file) {
  var deferred = q.defer();
  var promptMap = new Object();
  // console.log("function one");
  console.log(file);
  console.time("findVals");
  oboe(fs.createReadStream(jsonDir + file, {autoClose: true} ) )
  	.on('node', 
  		{
  			'{nodeId type}': function(item, path, ancestors) {
  				// console.log('=====================found node');
  				// console.log('len' + jsonMap.length);
    			_.each(jsonMap, function(ch) {
    				console.log(ch);
		        if (item.type === ch['nodeType'] && item.nodeId === ch['oldId']) {
		            // console.log(item);
		            console.log('===================================HIT: ' + file + ' ============= ' + ch['oldId']);
		            // console.log("FOUND: " + id);
		            console.dir(path);
		            console.timeEnd("findVals");
		        }
    			});
  		}}
  		).on('done', function (json) {
        console.log('request completed');
        console.timeEnd("findVals");
        // console.log(promptMap);
        console.log(json);
      });

      
      deferred.resolve();


  return deferred.promise;
};


// {
//         'prompts.*': function(prompt) {
//         	console.log(prompt);
//         	console.log(prompt.dimension);
//         	promptMap[prompt.promptId] = prompt.dimension;
//         }
// 			}
