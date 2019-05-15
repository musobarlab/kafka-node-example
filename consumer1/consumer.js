'use strict';

const kafka = require('kafka-node');
const Consumer = kafka.ConsumerGroup;

function KafkaConsumer(config) {
    let options = {
      kafkaHost: '127.0.0.1:9092',
      autoCommit: false,
      fetchMaxBytes: 10 * 1024 * 1024,
      groupId: config.groupId,
      sessionTimeout: 15000,
      protocol: ['roundrobin'],
      fromOffset: 'latest', // default
      encoding: 'utf8',
      keyEncoding: 'utf8'
    };
    return new Consumer(options, config.topic);
}

const consume = async (consumer) => {
    console.log('consumer 1 listening');

    let ctx = 'consume 1';

    consumer.on('message', (message) => {
        try {
            const messageValue = JSON.parse(message.value);
            console.log(messageValue);
            consumer.commit(true, (err, data) => {
                if (err) {
                    console.log(ctx, err, 'cannot commit message');
                } else {
                    console.log(ctx, data, 'message commited');
                }
            });
        } catch (error) {
            console.log(ctx, error);
        }
    });
};

module.exports = {
    consume,
    KafkaConsumer
};
  
