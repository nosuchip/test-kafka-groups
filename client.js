const { producer, topics } = require("./kafka");

const main = async () => {
  const topic = process.argv[2];
  const message = process.argv[3];

  if (!topics.includes(topic)) {
    console.error(`Invalid topic: ${topic}`);
    process.exit(1);
  }

  if (!message) {
    console.error(`Invalid message: ${message}`);
    process.exit(1);
  }

  await producer.connect();
  await producer.send({
    topic,
    messages: [{ value: message }],
  });
};

main()
  .then(() => {
    process.exit(0);
  })
  .catch(console.error);
