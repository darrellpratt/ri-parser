var amqp = require('amqp');
var path = require('path');
var fs = require('graceful-fs');
var oboe = require('oboe');
var q = require('q');
var couchbase = require('couchbase');
var JSON = require('JSON');
var _ = require('underscore');
var JSONStream = require('JSONStream');
var es = require('event-stream');

// where is data
var jsonDir = path.join(__dirname,'data/RI/');

// Load up the delta file into map of objects for checking later
var deltaList = new Array();
var deltaDimArray = new Array();

console.time('delta');
var _deltaJson = require('./data/NACNLSPI_20090501_500.json');
// oboe('./data/NACNLSPI_20090501_500.json').node('deltas.*', function(item) {
			// if (item.operation.toLowerCase() === 'update') {
_.each(_deltaJson.deltas, function(item) {
	var changeItem = new Object(); 
	changeItem['dim'] = item.dimension;
	changeItem['nodeType'] = item.type;
	changeItem['changedType'] = item.changedType;
	changeItem['newId'] = item.newId;
	changeItem['oldId'] = item.oldId;
	changeItem['newValue'] = item.newValue;
	changeItem['oldValue'] = item.oldValue;
	changeItem['operation'] = item.operation;
	deltaList.unshift(changeItem);
	deltaDimArray.unshift(item.dimension);
	console.log(changeItem);
});

console.log(_.uniq(deltaDimArray));


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
  				
  				
    			_.each(deltaList, function(ch) {
    				// console.log(item);
    				// console.log('>>' +  ch.nodeType);
    				// console.log(item.nodeId);
    				// console.log('>>' + ch.oldId);
    				// console.log(item.type + ":" + ch.nodeType + ":" + item.nodeId + ":" + ch.oldId);
		        if (item.type === ch.nodeType) {
		        		if (item.nodeId === ch.oldId) {
			            // console.log(item);
			            console.log('===================================HIT: ' + file + ' ============= ' + ch.oldId);
			            // console.log("FOUND: " + id);
			            // console.dir(path);
			            console.timeEnd("findVals");
			            this.abort();
		          }
		        }
    			});
    			
  		}}
  		).on('done', function (json) {
        console.log('request completed');
        console.timeEnd("findVals");
        // console.log(promptMap);
        console.log(json);
      });
	
	// var stream = fs.createReadStream(jsonDir + file, {encoding: 'utf8', autoClose: true}),
 //  parser = JSONStream.parse();

	// stream.pipe(parser);
	// parser.on('selections', function(data) {
	// 	console.log('==========================================================');
	//   console.log('received:', data);
	//   console.log('==========================================================');
	// });

	console.log('==========================================================');

	// q      
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
