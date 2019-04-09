const {JsonRpc} = require('eosjs');
const fetch = require('node-fetch');
const cluster = require('cluster');
const fs = require('fs');
const redis = require('redis');
const pmx = require('pmx');

const {
    getLastIndexedBlock,
    messageAllWorkers,
    printWorkerMap,
    getLastIndexedBlockFromRange,
    getLastIndexedBlockByDeltaFromRange,
    getLastIndexedBlockByDelta,
    getLastIndexedABI,
    onSaveAbi
} = require("./helpers/functions");

const {promisify} = require('util');
let client;
let cachedInitABI = null;

const missingRanges = [];

async function main() {
    // Preview mode - prints only the proposed worker map
    let preview = process.env.PREVIEW === 'true';

    const rClient = redis.createClient();
    const getAsync = promisify(rClient.get).bind(rClient);

    const n_deserializers = parseInt(process.env.DESERIALIZERS, 10);

    let max_readers = parseInt(process.env.READERS, 10);
    if (process.env.DISABLE_READING === 'true') {
        // Create a single reader to read the abi struct and quit.
        max_readers = 1;
    }
    const activeReaders = [];

    const eos_endpoint = process.env.NODEOS_HTTP;
    const rpc = new JsonRpc(eos_endpoint, {fetch});

    const queue_prefix = process.env.CHAIN;
    const queue = queue_prefix + ':blocks';
    const {index_queues} = require('./definitions/index-queues');

    const indicesList = ["action", "block", "abi", "delta"];

    const index_queue_prefix = queue_prefix + ':index';

    // Optional state tables
    if (process.env.ACCOUNT_STATE === 'true') {
        indicesList.push("table-accounts");
        index_queues.push({type: 'table-accounts', name: index_queue_prefix + "_table_accounts"});
    }

    if (process.env.VOTERS_STATE === 'true') {
        indicesList.push("table-voters");
        index_queues.push({type: 'table-voters', name: index_queue_prefix + "_table_voters"});
    }

    if (process.env.DELBAND_STATE === 'true') {
        indicesList.push("table-delband");
        index_queues.push({type: 'table-delband', name: index_queue_prefix + "_table_delband"});
    }
    if (process.env.USERRES_STATE === 'true') {
        indicesList.push("table-userres");
        index_queues.push({type: 'table-userres', name: index_queue_prefix + "_table_userres"});
    }

    const workerMap = [];
    let worker_index = 0;
    let pushedBlocks = 0;
    let consumedBlocks = 0;
    let indexedObjects = 0;
    let deserializedActions = 0;
    let lastProcessedBlockNum = 0;
    let total_read = 0;
    let total_blocks = 0;
    let total_indexed_blocks = 0;
    let total_actions = 0;
    let log_interval = 5000;
    let total_range = 0;
    let allowShutdown = false;
    let allowMoreReaders = true;
    let maxBatchSize = parseInt(process.env.BATCH_SIZE, 10);

    // Monitoring
    setInterval(() => {
        const _workers = Object.keys(cluster.workers).length;
        const tScale = (log_interval / 1000);
        total_read += pushedBlocks;
        total_blocks += consumedBlocks;
        total_actions += deserializedActions;
        total_indexed_blocks += indexedObjects;
        const log_msg = [
            `Workers: ${_workers}`,
            `Read: ${pushedBlocks / tScale} blocks/s`,
            `Consume: ${consumedBlocks / tScale} blocks/s`,
            `Deserialize: ${deserializedActions / tScale} actions/s`,
            `Index: ${indexedObjects / tScale} docs/s`,
            `${total_blocks}/${total_read}/${total_range}`
        ];

        console.log(log_msg.join(' | '));

        if (indexedObjects === 0 && deserializedActions === 0 && consumedBlocks === 0) {
            allowShutdown = true;
        }

        // reset counters
        pushedBlocks = 0;
        consumedBlocks = 0;
        deserializedActions = 0;
        indexedObjects = 0;

        if (_workers === 0) {
            console.log('FATAL ERROR - All Workers have stopped!');
            process.exit(1);
        }

    }, log_interval);


    // Start from the last indexed block
    let starting_block = 1;

    // Fecth chain lib
    const chain_data = await rpc.get_info();
    let head = chain_data['head_block_num'];

    if (process.env.STOP_ON !== "0") {
        head = parseInt(process.env.STOP_ON, 10);
    }

    if (process.env.START_ON !== "0") {
        starting_block = parseInt(process.env.START_ON, 10);
        // Check last indexed block again
        if (process.env.REWRITE !== 'true') {
            let lastIndexedBlockOnRange;

            if (lastIndexedBlockOnRange > starting_block) {
                console.log('WARNING! Data present on target range!');
                console.log('Changing initial block num. Use REWRITE = true to bypass.');
                starting_block = lastIndexedBlockOnRange;
            }
        }
        console.log('FIRST BLOCK: ' + starting_block);
        console.log('LAST  BLOCK: ' + head);
    }

    total_range = head - starting_block;
    // Create first batch of parallel readers
    let lastAssignedBlock = starting_block;
    let activeReadersCount = 0;

    if (process.env.REPAIR_MODE === 'false') {
        if (process.env.LIVE_ONLY === 'false') {
            while (activeReadersCount < max_readers && lastAssignedBlock < head) {
                worker_index++;
                const start = lastAssignedBlock;
                let end = lastAssignedBlock + maxBatchSize;
                if (end > head) {
                    end = head;
                }
                lastAssignedBlock += maxBatchSize;
                const def = {
                    worker_id: worker_index,
                    worker_role: 'reader',
                    first_block: start,
                    last_block: end
                };
                // activeReaders.push(def);
                activeReadersCount++;
                workerMap.push(def);
                // console.log(`Launching new worker from ${start} to ${end}`);
            }
        }

        // Setup Serial reader worker
        if (process.env.LIVE_READER === 'true') {
            const _head = chain_data['head_block_num'];
            console.log(`Starting live reader at head = ${_head}`);
            worker_index++;
            workerMap.push({
                worker_id: worker_index,
                worker_role: 'continuous_reader',
                worker_last_processed_block: _head,
                ws_router: ''
            });
        }
    }

    // Setup Deserialization Workers
    for (let i = 0; i < n_deserializers; i++) {
        for (let j = 0; j < process.env.DS_MULT; j++) {
            worker_index++;
            workerMap.push({
                worker_queue: queue + ":" + (i + 1),
                worker_id: worker_index,
                worker_role: 'deserializer'
            });
        }
    }

    // Setup ws router
    if (process.env.ENABLE_STREAMING) {
        worker_index++;
        workerMap.push({
            worker_id: worker_index,
            worker_role: 'router'
        });
    }

    // Quit App if on preview mode
    if (preview) {
        printWorkerMap(workerMap);
        process.exit(1);
    }

    // Launch all workers
    workerMap.forEach((conf) => {
        cluster.fork(conf);
    });

    if (!fs.existsSync('./logs')) {
        fs.mkdirSync('./logs');
    }

    const dsErrorsLog = './logs/' + process.env.CHAIN + "_ds_err_" + starting_block + "_" + head + ".txt";
    if (fs.existsSync(dsErrorsLog)) {
        fs.unlinkSync(dsErrorsLog);
    }
    const ds_errors = fs.createWriteStream(dsErrorsLog, {flags: 'a'});

    const cachedMap = await getAsync(process.env.CHAIN + ":" + 'abi_cache');
    let abiCacheMap;
    if (cachedMap) {
        abiCacheMap = JSON.parse(cachedMap);
        console.log(`Found ${Object.keys(abiCacheMap).length} entries in the local ABI cache`)
    } else {
        abiCacheMap = {};
    }

    setInterval(() => {
        rClient.set(process.env.CHAIN + ":" + 'abi_cache', JSON.stringify(abiCacheMap));
    }, 10000);

    // Worker event listener
    const workerHandler = (msg) => {
        switch (msg.event) {
            case 'init_abi': {
                if (!cachedInitABI) {
                    cachedInitABI = msg.data;
                    setTimeout(() => {
                        messageAllWorkers(cluster, {
                            event: 'initialize_abi',
                            data: msg.data
                        });
                    }, 1000);
                }
                break;
            }
            case 'router_ready': {
                messageAllWorkers(cluster, {
                    event: 'connect_ws'
                });
                break;
            }
            case 'save_abi': {
                onSaveAbi(msg.data, abiCacheMap, rClient);
                break;
            }
            case 'completed': {
                if (msg.id === doctorId.toString()) {
                    console.log('repair worker completed', msg);
                    console.log('queue size [before]:', missingRanges.length);
                    if (missingRanges.length > 0) {
                        const range_data = missingRanges.shift();
                        console.log('New repair range', range_data);
                        console.log('queue size [after]:', missingRanges.length);
                        doctorIdle = false;
                        messageAllWorkers(cluster, {
                            event: 'new_range',
                            target: msg.id,
                            data: {
                                first_block: range_data.start,
                                last_block: range_data.end
                            }
                        });
                    } else {
                        doctorIdle = true;
                    }
                } else {
                    activeReadersCount--;
                    if (activeReadersCount < max_readers && lastAssignedBlock < head && allowMoreReaders) {
                        // Assign next range
                        const start = lastAssignedBlock;
                        let end = lastAssignedBlock + maxBatchSize;
                        if (end > head) {
                            end = head;
                        }
                        lastAssignedBlock += maxBatchSize;
                        const def = {
                            first_block: start,
                            last_block: end
                        };
                        activeReadersCount++;
                        messageAllWorkers(cluster, {
                            event: 'new_range',
                            target: msg.id,
                            data: def
                        });
                    }
                }
                break;
            }
            case 'add_index': {
                indexedObjects += msg.size;
                break;
            }
            case 'ds_action': {
                deserializedActions++;
                break;
            }
            case 'ds_error': {
                ds_errors.write(msg.gs + '\n');
                break;
            }
            case 'read_block': {
                pushedBlocks++;
                break;
            }
            case 'consumed_block': {
                consumedBlocks++;
                if (msg.block_num > lastProcessedBlockNum) {
                    lastProcessedBlockNum = msg.block_num;
                }
                break;
            }
        }
    };

    // Attach handlers
    for (const c in cluster.workers) {
        if (cluster.workers.hasOwnProperty(c)) {
            const self = cluster.workers[c];
            self.on('message', (msg) => {
                workerHandler(msg, self);
            });
        }
    }

    pmx.action('stop', (reply) => {
        allowMoreReaders = false;
        console.info('Stop signal received. Shutting down readers immediately!');
        console.log('Waiting for queues...');
        reply({
            ack: true
        });
        setInterval(() => {
            if (allowShutdown) {
                console.log('Shutting down master...');
                rClient.set('abi_cache', JSON.stringify(abiCacheMap));
                process.exit(1);
            }
        }, 500);
    });

}

module.exports = {main};
