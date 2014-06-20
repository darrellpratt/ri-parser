var amqp = require('amqp'),
    ampqplib = require('amqplib'),
    when = require('when'),
    path = require('path'),
    fs = require('graceful-fs'),
    oboe = require('oboe'),
    // q = require('q'),
    JSON = require('JSON'),
    _ = require('underscore'),
    JSONStream = require('JSONStream'),
    es = require('event-stream')
    chalk = require('chalk'),
    couchbase = require('couchbase');

var db = new couchbase.Connection({
    host: '10.14.31.24:8091',
    bucket: 'riparser'
});


// where is data
var jsonDir = path.join(__dirname, 'data/RI/');

// counters
var hitCount = 0;
var missCount = 0;

// Load up the delta file into map of objects for checking later
var deltaStruct = new Object(); // would like to key by dim here eventually
var deltaList = new Array();


var deltaDimArray = new Array();
var uniqDeltaDimArray = new Array();

var _deltaJson = require('./data/NACNLSPI_20090501_500.json');

_.each(_deltaJson.deltas, function(item) {
    var changeItem = {
        'dim': item.dimension,
        'type': item.type,
        'changedType': item.changedType,
        'newId': item.newId,
        'oldId': item.oldId,
        'newValue': item.newValue,
        'oldValue': item.oldValue,
        'operation': item.operation
    };
    // console.log(changeItem.changedType);
    if (_.isUndefined(deltaStruct[changeItem.type])) {
        deltaStruct[changeItem.type] = new Array();
    };
    deltaStruct[changeItem.type].unshift(changeItem);
    // console.log(deltaStruct);
    // deltaList.unshift(changeItem);
    deltaDimArray.unshift(item.dimension);
    // console.log(changeItem);
});

// keep track of actual dims in the change set delta, used later in checks
uniqDeltaDimArray = _.uniq(deltaDimArray);
console.log(uniqDeltaDimArray);
console.log(deltaStruct);

// read from queue for items
var connection = amqp.createConnection({
    host: 'localhost'
});
connection.on('ready', function() {
    connection.queue('task_queue', {
        autoDelete: false,
        durable: true
    }, function(queue) {

        console.log(' [*] Waiting for messages. To exit press CTRL+C');

        queue.subscribe({
            ack: true,
            prefetchCount: 1
        }, function(msg) {
            var body = msg.data.toString('utf-8');
            console.log(' [x] Received %s', body);
            findVals(body);
            setTimeout(function() {
                console.log(' [x] Done');
                console.log('Hits: ' + hitCount + ' misses: ' + missCount);
                queue.shift(); // basic_ack equivalent
            }, (body.split('.').length - 1) * 10);
        });
    });
});

// search the RI for changes

function findVals(file) {
    // var deferred = q.defer();
    var promptMap = new Object();
    logging(file);


    var foundChange = false;
    oboe(fs.createReadStream(jsonDir + file, {
        autoClose: true
    }))
        .on('node', {
            '{nodeId type}': function(item, path, ancestors) {
                // console.log(item.type);

                _.each(deltaStruct[item.type], function(change) {
                    // console.log(item.type + ":" + change.nodeType + ":" + item.nodeId + ":" + change.oldId);
                    if (item.type === change.type) {
                        // TODO: check for which type of val changed here
                        if (item.nodeId === change.oldId) {
                            // console.log(item);
                            console.log(chalk.blue('================HIT: ' + file + ' ============= ' + change.oldId));
                            foundChange = true;
                            this.abort();
                        }
                    }
                });

            }
        }).on('done', function(json) {
            console.log('request completed');
            console.log(foundChange);
            if (foundChange) {
                console.log(json);
                console.log(chalk.bgCyan(file + '+++++++++++++++++++++++HITHITHIT'));
                hitCount++;
                // writeDirtyTask(file).then(function () {
                //   console.log(chalk.bgBlue('+++++++++++++++++++++++DONE'));
                // });
                // write new file
                fs.writeFile('./data/dirty/' + file, json, function(err) {
                    if (err) throw err;
                    console.log('It\'s saved!');
                });
            } else {
                console.log(chalk.bgRed(file + '***********************NO HIT'));
                missCount++;
            };

            
            var rnd = 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function(c) {
                var r = Math.random() * 16 | 0,
                    v = c == 'x' ? r : (r & 0x3 | 0x8);
                return v.toString(16);
            });
            var id = file.substring(0, file.indexOf('\.json')) + rnd;
            db.set(id, json, {
                expiry: 0
            }, function(err, result) {
                if (err) {
                    console.log("Error on saving couchbase item");
                    console.log(err);
                }
            })


        });

    // q
    // deferred.resolve();

    // return deferred.promise;
};

function writeDirtyTask(file) {
    console.log(chalk.bgYellow('into rabbit new task'));
    amqp.connect('amqp://localhost').then(function(conn) {
        return when(conn.createChannel().then(function(ch) {
            var q = 'fix_queue';
            var ok = ch.assertQueue(q, {
                durable: true
            });

            return ok.then(function() {
                var msg = file;
                ch.sendToQueue(q, new Buffer(msg), {
                    deliveryMode: true
                });
                console.log(chalk.bgGreen(" [x] Sent '%s'", msg));
                return ch.close();
            });
        })).ensure(function() {
            conn.close();
        });
    }).then(null, console.warn);
}

function logging(tick) {
    console.log();
    console.log();
    console.log(chalk.green('--------------------------------------------------'));
    console.log(chalk.blue(tick));
    console.log(chalk.green('--------------------------------------------------'));
    console.log();
    console.log();
}


// {
//         'prompts.*': function(prompt) {
//          console.log(prompt);
//          console.log(prompt.dimension);
//          promptMap[prompt.promptId] = prompt.dimension;
//         }
//      }
