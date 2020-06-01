const { URL } = require('url');
const amqplib = require('amqplib');
const fetch = require('node-fetch');
const yargs = require('yargs');

// connection details
const host = 'localhost';
const port = 5672;
const mgmtPort = 15672;
const username = 'guest';
const password = 'guest';

const QUEUE_NAME = 'testQueue';

// Parameters

const PUBLISH_SLEEP_MS = 0;
const PUBLISHER_HEAD_START_MS = 100; // let the queue fill up a bit before reading

const CONSUMER_MAX_BUFFER_LENGTH = 50;

const RUN_LOOP_SLEEP_MS = 100; // time for "processing" for each message
const RUN_LOOP_CHECKPOINT_INTERVAL_MS = 5000;

const STATS_LOOP_SLEEP_MS = 500;

// State

/** @type {Publisher} */
let publisher;
/** @type {BackpressureAwareConsumer} */
let consumer;
/** @type {Set<string>} */
let sessionIds;
/** @type {Array} */
let sessionMessages;

let isRunning = true;

let shouldPurgeQueue = false;

// Utilities

const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

const round2 = (num) => Math.round((num + Number.EPSILON) * 100) / 100;

function getConnectionURI() {
  const url = new URL(`amqp://${host}`);
  url.username = username;
  url.password = password;
  url.port = `${port}`;
  return url.toString();
}

async function declareQueue(channel) {
  // set up a queue with same defaults as RMQ connector
  await channel.assertQueue(QUEUE_NAME, {
    durable: true,
    exclusive: false,
    autoDelete: false,
    arguments: null,
  });
  if (shouldPurgeQueue) {
    // just in case there are lingerers from a previous run
    console.log('Purging queue');
    await channel.purgeQueue(QUEUE_NAME);
  }
}

// Stats

async function sampleStats() {
  const mgmtUrl = new URL(`http://${host}:${mgmtPort}/api/queues/${encodeURIComponent('/')}/${QUEUE_NAME}`);
  mgmtUrl.username = username;
  mgmtUrl.password = password;
  const res = await fetch(mgmtUrl);
  const queue = await res.json();
  const approxUnacked = consumer.buffer.length + sessionIds.size;
  console.debug('stats', JSON.stringify({
    ts: new Date(),
    queue: {
      unacked: queue.messages_unacknowledged,
      ready: queue.messages_ready,
      'publish/s': queue.message_stats && queue.message_stats.publish_details.rate,
      'deliver/s': queue.message_stats && queue.message_stats.deliver_details.rate,
    },
    consumer: {
      'buffer length': consumer.buffer.length,
      remaining: CONSUMER_MAX_BUFFER_LENGTH - consumer.buffer.length,
      prefetch: consumer.prefetchAmount,
    },
    session: {
      'session length': sessionIds.size,
    },
    unacked: {
      observed: queue.messages_unacknowledged,
      estimate: approxUnacked,
      delta: queue.messages_unacknowledged - approxUnacked,
      '% error': round2((queue.messages_unacknowledged - approxUnacked) / queue.messages_unacknowledged * 100),
    },
  }));
}

async function statsLoop() {
  while (isRunning) {
    await sampleStats();
    await sleep(STATS_LOOP_SLEEP_MS);
  }
}

// Message generation

class Publisher {
  constructor(connection, channel) {
    this.connection = connection;
    this.channel = channel;
    this.isRunning = true;
    this.idCounter = 0;
  }

  async sendMessage() {
    const message = Buffer.from('read as ASAP as possible, then burn');
    this.idCounter += 1;
    console.log(`Publishing message ${this.idCounter}`);
    const pubBuffHasSpace = this.channel.sendToQueue(QUEUE_NAME, message, {
      correlationId: `${this.idCounter}`,
    });
    if (!pubBuffHasSpace) {
      await this.drainChannel();
    }
  }

  async drainChannel() {
    return new Promise(((resolve) => {
      this.channel.once('drain', resolve);
    }));
  }

  async publish() {
    // send x messages/ second
    while (this.isRunning) {
      await publisher.sendMessage();
      await sleep(PUBLISH_SLEEP_MS);
    }
  }
}

// BackpressureAwareConsumer prototype

class BackpressureAwareConsumer {
  // need to set prefetch in scale with the message buffer
  // need to allow one message to be pulled at a time from the buffer

  constructor(connection, channel) {
    this.buffer = [];
    this.maxBufferLength = CONSUMER_MAX_BUFFER_LENGTH;
    this.connection = connection;
    this.channel = channel;
    // if there is still room in the buffer
    // allow as many as possible in
    // NOTE: need to handle v3.3.0 global semantics: https://www.rabbitmq.com/consumer-prefetch.html
    this.prefetchAmount = CONSUMER_MAX_BUFFER_LENGTH;
    this.channel.prefetch(this.prefetchAmount);
  }

  consume() {
    this.channel.consume(QUEUE_NAME, (msg) => {
      // TODO: incorporate variable prefetch amount
      this.buffer.push(msg);
    });
  }

  getNextMessage() {
    // lifo queue
    return this.buffer.shift();
  }
}

// Lifecycle methods

// open the connections to Rabbit for both a publisher and a consumer
async function open() {
  sessionIds = new Set();
  sessionMessages = [];

  // use different connections for message generation/ consumption
  const pubConn = await amqplib.connect(getConnectionURI());
  const pubChan = await pubConn.createChannel();
  publisher = new Publisher(pubConn, pubChan);

  const consConn = await amqplib.connect(getConnectionURI());
  const consChan = await consConn.createChannel();
  consumer = new BackpressureAwareConsumer(consConn, consChan);

  await declareQueue(consChan);
}

// The main run loop, as in the RMQ source
async function run() {
  // pull off the next message, add to checkpoint buffer
  // ack all message in buffer every x seconds
  const msg = consumer.getNextMessage();
  if (!msg) {
    // no messages ready
    console.log(`no available messages`);
    return;
  }

  const { correlationId } = msg.properties;
  if (sessionIds.has(correlationId)) {
    return;
  }

  sessionIds.add(correlationId);
  sessionMessages.push(msg);
  // collect the message
  console.log('Processed message', correlationId);
}

async function ackSessionIds() {
  // no support for txCommit, but shouldn't be a problem here
  console.log(`Acking ${sessionIds.size} messages`);
  for (const msg of sessionMessages) {
    const { correlationId } = msg.properties;
    console.log('Acked message', correlationId);
    consumer.channel.ack(msg);
  }
  sessionIds.clear();
  sessionMessages = [];
}

async function runLoop(onErr) {
  consumer.consume();

  publisher
    .publish()
    .catch(onErr);

  await sleep(PUBLISHER_HEAD_START_MS);

  statsLoop()
    .catch(onErr);

  let lastCheckpointTime = Date.now();

  let runNum = 0;
  while (isRunning) {
    runNum += 1;
    try {
      console.log(`running ${runNum}`);
      await run();
      await sleep(RUN_LOOP_SLEEP_MS);
      const now = Date.now();
      if (lastCheckpointTime + RUN_LOOP_CHECKPOINT_INTERVAL_MS < now) {
        console.log(`checkpointing run ${runNum}`);
        await ackSessionIds();
        lastCheckpointTime = now;
      }
    } catch (err) {
      onErr(err);
    }
  }
}

async function close() {
  return Promise.allSettled([
    publisher.connection.close(),
    consumer.connection.close(),
  ]);
}

// main entrypoint

(async () => {
  const args = yargs
    .option('purge-queue', {
      alias: ['purge', 'p'],
      type: 'boolean',
      default: false,
    })
    .parse();

  shouldPurgeQueue = args['purge-queue'];

  const onErr = (err) => {
    if (err) {
      console.error(err);
    }
    isRunning = false;
    publisher.isRunning = false;
  };
  process.once('SIGINT', e => onErr(e));
  await open();
  await runLoop(onErr);
  await close();
  console.log('Connections closed');
  await sampleStats();
})();
