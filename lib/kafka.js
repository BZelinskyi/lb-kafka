const { Kafka } = require("kafkajs");
const fs = require("node:fs");
const debug = require("debug")("loopback:connector:kafka");

class kafkaConnector {
	constructor(kafka) {
		this.kafka = kafka;
		this.producer = kafka.producer();
	}

	connectProducer() {
		debug("loopback:connector:kafka:connectProducer");
		return this.producer.connect();
	}

	send(payload) {
		debug(payload, "loopback:connector:kafka:payload");
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
	const { brokers, name, sasl, caPath, enableKafka } = dataSource.settings;
	if (enableKafka == "false") return callback(null, {});
	let options = { name, brokers: brokers.split(",") };
	if (typeof caPath === "string") options.ssl = { rejectUnauthorized: false, ca: [fs.readFileSync(caPath, "utf-8")] };
	if (typeof sasl === "object") options.sasl = sasl;
	const connector = new Kafka(options);
	dataSource.connector = new kafkaConnector(connector);
	dataSource.connector.dataSource = dataSource;
	debug(options, "loopback:connector:kafka:options");
	if (callback) dataSource.connector.connectProducer(callback);
};
