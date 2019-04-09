const async = require('async');
const pmx = require("pmx");
const {amqpConnect} = require("../connections/rabbitmq");

let ch;

const indexingPrefecthCount = parseInt(process.env.INDEX_PREFETCH, 10);

async function run() {
    [ch,] = await amqpConnect();
    try {
        ch.prefetch(indexingPrefecthCount);
        ch.assertQueue(process.env['queue'], {durable: true});
        console.log(`setting up indexer on queue ${process.env['queue']}`);
    } catch (e) {
        console.error('elasticsearch cluster is down!');
        process.exit(1);
    }

    pmx.action('stop', (reply) => {
        ch.close();
        reply({
            event: 'index_channel_closed'
        });
    });
}

module.exports = {run};
