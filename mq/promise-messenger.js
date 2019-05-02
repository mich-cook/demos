#!/usr/bin/env node

//
// top level stuff used throughout
//
var queueName = 'message-demo';
var queueConnection;
var queueConnectionPromise = require('amqplib').connect('amqp://localhost');

var producers = [];
var consumers = [];

var producerChannel;
var consumerChannel;

//
// Setting up the connection and channels to the message queue
//
queueConnectionPromise.then(function(connection) {
	console.log(`Connection to message queue established.`);
	queueConnection = connection;

	// set up the channel for producers
	// bail if it breaks
	var producerChannelPromise = connection.createChannel()
		.then(function(channel) { producerChannel = channel; console.log(`Producer channel established.`); })
		.catch(function() { shutdown(2, `Failed to open channel for producers.`); });

	// set up the channel for consumers (even though there's just one for now)
	// again, bail if it breaks
	var consumerChannelPromise = connection.createChannel()
		.then(function(channel) { consumerChannel = channel; console.log(`Consumer channel established.`); })
		.catch(function() { shutdown(2, `Failed to open channel for consumers.`);  });

	// channels are all established, let's have some fun sending messages
	Promise.all([producerChannelPromise, consumerChannelPromise]).then(function() {
		startup();
	});

}).catch(function(error) { shutdown(1, `Failed to open connection to message queue.`); });

//
// Message producer and associated functions
//
function Producer() {
	this.message = `Hello Demo!`;
	this.slot = `TBD`;
	this.intervalHandle;
	this.start = function() {
		producerChannel.assertQueue(queueName, { durable: false });
		this.intervalHandle = setInterval(function() {
			var timestamp = new Date().getTime();
			var payload = { "message": this.message, "timestamp": timestamp, "slot": this.slot };
			producerChannel.sendToQueue(queueName, Buffer.from(JSON.stringify(payload)));
		}.bind(this), 500);
	};
	this.stop = function() {
		clearInterval(this.intervalHandle);
	};
};

// makes a new one, starts it, then puts it in the pile with the rest
function createProducer() {
	var producer = new Producer();
	producer.start();
	producer.slot = producers.push(producer);
	console.log(`Created a producer. We now have ${producers.length}.`);
};

// takes the last one we created and kills it
function destroyProducer() {
	if (producers.length === 0) {
		console.log(`No producers to remove.`);
		return;
	}
	var producer = producers.pop();
	producer.stop();  // stop the interval message production
	producer = undefined;  // come get me, gc
	console.log(`Removed a producer. We now have ${producers.length}.`);
};

//
// Message consumer and associated functions
//
function Consumer() {
	this.start = function() {
		return consumerChannel.assertQueue(queueName, { durable: false }).then(function() {
			return consumerChannel.consume(queueName, function(message) {
				var msg = JSON.parse(message.content.toString());
				console.log(`\tReceived message '${msg.message}' from producer ${msg.slot} with timestamp ${msg.timestamp}.`);
				consumerChannel.ack(message);
			});
		});
	};
};

// TODO: consumer functions if we really want get to them

// once we have the channels, we'll start the consumer and
// the interval that creates/destroys producers at random
// every three seconds. that's usually enough time to see
// that the producers have changed by watching the slot numbers
function startup() {

	// right now we just create one consumer.
	// since we're not doing much with the messages the
	// single consumer can actually handle a bunch of
	// producer instances
	(function() {
		var consumer = new Consumer();
		consumer.start()
				.then(function() {
					consumers.push(consumer);
					console.log(`Created a consumer.`);
				});

		// the fun part. randomly create or destroy producers
		// that will produce messages for our consumer. to
		// some extent, it kinda shows how it would work
		// if these were spread across some set of machine
		// instances.
		setInterval(function() {
			if (Math.random() > 0.5) {
				createProducer();
			} else {
				destroyProducer();
			}
		}, 3000);

	}());

};

// close up shop as cleanly as we can.
// TODO: kill all the producers
// TODO: drain the message queue and kill the consumer
function shutdown(code = 0, message) {

	// if we want to provide a reason that we're shutting down
	if (message !== undefined) {
		console.log(message);
	}

	console.log(`Shooting the messenger.`);

	// close the channels
	if (producerChannel !== undefined) {
		console.log(`Closing the producer channel.`);
		producerChannel.close();
	}

	if (consumerChannel !== undefined) {
		console.log(`Closing the consumer channel.`);
		consumerChannel.close();
	}

	// close the connection
	if (queueConnection !== undefined) {
		console.log(`Closing the connection to the message queue.`);
		queueConnection.close();
	}

	// and we're done.
	process.exit(code);

};

process.on('SIGINT', function() {
	shutdown(0, `\nCaught a ctrl-c`);
});
