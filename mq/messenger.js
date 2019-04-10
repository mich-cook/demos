#!/usr/bin/env node

var queueName = 'message-demo';
var queueConnection;

var producers = [];
var consumers = [];

var producerChannel;
var consumerChannel;

require('amqplib/callback_api')
	.connect('amqp://localhost', function(error, connection) {
		if (error !== null) { shutdown(1, `Failed to open connection to message queue.`); }
		console.log(`Connection to message queue established.`);
		queueConnection = connection;

		connection.createChannel(function(error, channel) {
			if (error !== null) { shutdown(2, `Failed to open channel for producers.`); }
			console.log(`Producer channel established.`);
			producerChannel = channel;
			startup();
		});

		connection.createChannel(function(error, channel) {
			if (error !== null) { shutdown(2, `Failed to open channel for consumers.`); }
			console.log(`Consumer channel established.`);
			consumerChannel = channel;
			startup();
		});

	});

function Producer() { };

function Consumer() { };

function startup() {
	// wait for the second knock
	// each channel will call startup when it's created
	// since we can't know how long each will take
	// first will fail this test. second will succeed.
	if ((consumerChannel === undefined) || (producerChannel === undefined)) {
		return;
	}

};

function shutdown(code = 0, message) {

	if (message !== undefined) {
		console.log(message);
	}

	console.log(`Shooting the messenger.`);

	if (producerChannel !== undefined) {
		console.log(`Closing the producer channel.`);
		producerChannel.close();
	}

	if (consumerChannel !== undefined) {
		console.log(`Closing the consumer channel.`);
		consumerChannel.close();
	}

	if (queueConnection !== undefined) {
		console.log(`Closing the connection to the message queue.`);
		queueConnection.close();
	}

	process.exit(code);

};

process.on('SIGINT', function() {
	console.log(`\nCaught a ctrl-c`);
	shutdown();
});
