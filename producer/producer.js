'use strict';

const kafka = require('kafka-node');
const ctx = 'kafka-producer';

function KafkaProducer() {
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
      
    return new kafka.HighLevelProducer(client);
}

const produce = (producer, data, cb) => {
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
            cb(err);
        } else {
            cb(null, data);
        }
    });
};

module.exports = {
    produce,
    KafkaProducer
};
