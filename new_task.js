#!/usr/bin/env node
// Post a new task to the work queue

var amqp = require('amqplib');
var when = require('when');

amqp.connect('amqp://localhost').then(function(conn) {
  return when(conn.createChannel().then(function(ch) {
    console.log('created ch');
    var q = 'fix_queue';
    var ok = ch.assertQueue(q, {durable: true});
    
    return ok.then(function() {
      console.log('into then');
      var msg = process.argv.slice(2).join(' ') || "Hello World!";
      ch.sendToQueue(q, new Buffer(msg), {deliveryMode: true});
      console.log(" [x] Sent '%s'", msg);
      return ch.close();
    });
  })).ensure(function() { conn.close(); console.log('closing'); });
}).then(null, console.warn);