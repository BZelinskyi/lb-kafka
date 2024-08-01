const { Kafka } = require("kafkajs");
const fs = require("node:fs");

class kafkaConnector {
	constructor(kafka, options) {
		this.kafka = kafka;
		this.producer = kafka.producer();
		this.options = options;
	}

	connectProducer() {
		return this.producer.connect().then(succ => Promise.resolve(this.options)).catch(succ => Promise.reject(this.options))
	}

	send(payload) {
		return this.producer.send(payload);
	}

	sendBatch(batch) {
		return this.producer.sendBatch(batch);
	}

	disconnect() {
		return this.producer.disconnect();
	}
}

exports.initialize = function initializeDataSource(dataSource, callback) {
	const { brokers, name, sasl,caPath } = dataSource.settings;
	const options= {
		name,
		brokers: brokers.split(","),
		sasl,
		ssl: {
			rejectUnauthorized: false,
			ca: [fs.readFileSync(caPath, "utf-8")],
		},
	}
	const connector = new Kafka(options);
	dataSource.connector = new kafkaConnector(connector, options);
	if (callback) callback(null, dataSource.connector);
};
