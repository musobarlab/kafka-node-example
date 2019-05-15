'use strict';

const express = require('express');
const {produce, KafkaProducer} = require('./producer');
const bodyParser = require('body-parser');

const app = express();

//setup kafka producer

const producer = new KafkaProducer();
producer.on('ready', () => {
    console.log('kafka producer is connected');
});

producer.on('error', (error) => {
    console.log('Kafka Producer Error', error);
});

app.use(bodyParser.json());
app.use(bodyParser.urlencoded({     // to support URL-encoded bodies
    extended: true
}));

app.get('/', (req,res) => {
    res.json({greeting:'kafka producer'})
});

app.post('/send', (req, res) => {

    const body = req.body;

    const message = {
        topic : 'test1',
        body: body
    };

    produce(producer, message, (err, data) => {
        if (err) {
            console.log('producer send error');
        } else {
            console.log(`producer send success ${data}`);
        }
    });
    
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