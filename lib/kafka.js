const { Kafka } = require("kafkajs");

class kafkaConnector {
	constructor(kafka, settings) {
		this.kafka = kafka;
		this.producer = kafka.producer();
	}

	connectProducer() {
		return this.producer.connect();
	}

	send(topic, messages) {
		return this.producer.send({ topic, messages });
	}

	disconnect() {
		return this.producer.disconnect();
	}
}

exports.initialize = function initializeDataSource(dataSource, callback) {
	const settings = dataSource.settings;
	const connector = new Kafka(settings);
	dataSource.connector = new kafkaConnector(connector, settings);
	if (callback) callback(null, dataSource.connector);
};
