const { Kafka } = require("kafkajs");

class kafkaConnector {
	constructor(kafka, settings) {
		this.kafka = kafka;
		this.producer = kafka.producer();
		this.consumer = kafka.consumer({
			groupId: settings.groupId
		});
	}

	connectProducer() {
		return this.producer.connect();
	}

	connectConsumer() {
		return this.consumer.connect();
	}
  
	async subscribeTopic({ topic, ...others}, cb) {
		await this.consumer.subscribe({ topic, others });
		this.consumer.run({
			eachMessage: async ({ topic, partition, message }) => {
				cb({
					topic,
					partition,
					offset: message.offset,
					value: message.value.toString(),
				});
			},
		});
	}

	send(topic, messages) {
		return this.producer.send({ topic, messages });
	}


	disconnect() {
		return Promise.all[this.consumer.disconnect(), this.producer.disconnect()];
	}
}

exports.initialize = function initializeDataSource(dataSource, callback) {
	const settings = dataSource.settings;
	const connector = new Kafka(settings);
	dataSource.connector = new kafkaConnector(connector, settings);
	if (callback) callback(null, dataSource.connector);
};
