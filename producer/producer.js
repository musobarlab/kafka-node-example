'use strict';

const kafka = require('kafka-node');
const ctx = 'kafka-producer';

const client = new kafka.KafkaClient({
  kafkaHost: '127.0.0.1:9092',
  autoConnect: true,
  connectRetryOptions: {
    retries: 4,
    minTimeout: 1000,
    maxTimeout: 3000,
    randomize: true
  }
});

const producer = new kafka.HighLevelProducer(client);
producer.on('ready', () => {
    console.log(ctx, 'ready', 'kafka producer is connected');
});

const produce = (data) => {
  const buffer = new Buffer.from(JSON.stringify(data.body));
  const record = [
    {
      topic: data.topic,
      messages: buffer,
      attributes: 1
    }
  ];
  producer.send(record, (err, data) => {
    if(err) {
      console.log(ctx, 'producer send error', 'kafka-producer');
    } else {
        console.log(ctx,`producer send success ${data}`,'kafka-producer');
    }
  });
};

producer.on('error', (error) => {
    console.log(ctx, error, 'Kafka Producer Error');
});


module.exports = {
    produce
};
