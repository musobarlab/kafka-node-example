'use strict';

const express = require('express');
const kafkaProducer = require('./producer');
const bodyParser = require('body-parser');

const app = express();

app.use(bodyParser.json());
app.use(bodyParser.urlencoded({     // to support URL-encoded bodies
    extended: true
}));

const kafkaSendMessage = async (data) => {
    const message = {
      topic : 'test1',
      ...data
    };
    kafkaProducer.produce(message);
  };
  

app.get('/', (req,res) => {
    res.json({greeting:'kafka producer'})
});

app.post('/send', (req, res) => {

    const body = req.body;
    const message = {
        body: body
    };

    kafkaSendMessage(message);
    
    res.json(req.body);
});

const server = app.listen(9000, () => {
    console.log('Kafka producer running at 9000');
});

// handle event
// Using a single function to handle multiple signals
function handle(signal) {
    console.log(`received ${signal}`);
    server.close(() => {
        console.log('server closed');
        process.exit(0);
    });
}
  
process.on('SIGINT', handle);
process.on('SIGTERM', handle);