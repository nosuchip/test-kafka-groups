const { Kafka } = require("kafkajs");

const client = new Kafka({
  // logCreator: (logLevel) => (entry) => {},
  clientId: "test-client",
  brokers: ["127.0.0.1:9094"],
  retry: {
    initialRetryTime: 100,
    factor: 1.5,
  },
});

const producer = client.producer({
  allowAutoTopicCreation: true,
});

const topics = ["topic1", "topic2", "topic3", "topic4", "topic5"];
const groups = ["group-1", "group-2", "group-3"];

const emit = async (topic, message) => {
  console.log(`Emitting topic: ${topic}: message: ${message}`);

  await producer.send({
    topic: topic,
    messages: [{ value: "Message " + message }],
  });
};

module.exports = { client, producer, topics, groups, emit };
