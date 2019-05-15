'use strict';

const express = require('express');
const {consume, KafkaConsumer} = require('./consumer');
const bodyParser = require('body-parser');

const app = express();

app.use(bodyParser.json());
app.use(bodyParser.urlencoded({     // to support URL-encoded bodies
    extended: true
}));

const consumerConfig = {
    topic: 'test1',
    groupId: 'group1'
};

// setup kafka consumer
const consumer = new KafkaConsumer(consumerConfig);

consume(consumer);

app.get('/', (req,res) => {
    res.json({greeting:'kafka consumer 1'})
});


const server = app.listen(9001, () => {
    console.log('Kafka consumer 1 running at 9001');
});

// handle event
// Using a single function to handle multiple signals
function handle(signal) {
    console.log(`received ${signal}`);
    server.close(() => {
        console.log('server closed');
        consumer.close(true, () => {
            console.log('consumer closed');
            process.exit(0);
        });
    });
}
  
process.on('SIGINT', handle);
process.on('SIGTERM', handle);