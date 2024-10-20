const { client, producer, groups, topics } = require("./kafka");

const createConsumer = async (consumerId, group, topic) => {
  console.log(`[consumer-${consumerId}] | [${group}] | [${topic}]: Creating`);
  const consumer = client.consumer({ groupId: group });

  await consumer.connect();
  await consumer.subscribe({ topic });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      await new Promise((resolve) => setTimeout(resolve, consumerId * 500));

      console.log(
        `[consumer-${consumerId}] | [${group}] | [${topic}] | [${partition}]: ${message.value?.toString()} @ ${
          message.offset
        }`
      );

      // consumer.commitOffsets([{ topic, partition, offset: message.offset }]);
    },
  });
};

const main = async () => {
  await Promise.all([
    createConsumer(1, groups[0], topics[0]),
    createConsumer(2, groups[0], topics[1]),
    createConsumer(3, groups[1], topics[0]),
    createConsumer(4, groups[1], topics[1]),
    createConsumer(5, groups[1], topics[2]),
    createConsumer(6, groups[2], topics[0]),
    createConsumer(7, groups[2], topics[2]),
    createConsumer(8, groups[2], topics[2]),
  ]).catch(console.error);

  await producer.connect();
};

main().catch(console.error);
