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
const PUBLISH_HEAD_START_MS = 1000;
const PUBLISH_SLEEP_MS = 18;

const CONSUMER_MAX_BUFFER_LENGTH = 500;

//// Periodic backpressuring
const RUN_LOOP_SLEEP_MS = 15; // base duration for "processing" for each message
const RUN_LOOP_SINE_COEFF = 10;
const RUN_LOOP_SINE_ANGLE_COEFF = 0.25;

//// Checkpointing
const CHECKPOINT_INTERVAL_MS = 10000;
const CHECKPOINT_POLL_MS = 500;

//// Stats sampling
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

let lastProcessingDuration = 0;

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
      'publish/s': !queue.message_stats ? 'N/A' : queue.message_stats.publish_details.rate,
      'deliver/s': !queue.message_stats ? 'N/A' : queue.message_stats.deliver_details.rate,
      'delta/s': !queue.message_stats ? 'N/A' : round2(queue.message_stats.publish_details.rate - queue.message_stats.deliver_details.rate),
    },
    consumer: {
      'processing dur': round2(lastProcessingDuration),
      'buffer length': consumer.buffer.length,
      'buffer space': CONSUMER_MAX_BUFFER_LENGTH - consumer.buffer.length,
      prefetch: consumer.prefetchCount,
      'prefetch space': consumer.prefetchCount - queue.messages_unacknowledged,
    },
    session: {
      'session length': sessionIds.size,
    },
    unacked: {
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
    if (this.idCounter % 1000 === 0) {
      console.log(`Publishing message ${this.idCounter}`);
    }
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
    this.prefetchCount = CONSUMER_MAX_BUFFER_LENGTH;
    this.channel.prefetch(this.prefetchCount, false); // not global
  }

  consume() {
    this.channel.consume(QUEUE_NAME, async (msg) => {
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

async function checkpointPollingLoop() {
  let lastCheckpointTime = Date.now();
  while (isRunning) {
    await sleep(CHECKPOINT_POLL_MS);
    const now = Date.now();
    if (lastCheckpointTime + CHECKPOINT_INTERVAL_MS < now) {
      console.log('checkpointing');
      await ackSessionIds();
      lastCheckpointTime = now;
    }
  }
}

async function runLoop(onErr) {
  statsLoop()
    .catch(onErr);

  checkpointPollingLoop()
    .catch(onErr);

  publisher
    .publish()
    .catch(onErr);

  await sleep(PUBLISH_HEAD_START_MS);

  consumer.consume();

  let runNum = 0;
  while (isRunning) {
    runNum += 1;
    try {
      console.log(`running ${runNum}`);
      await run();
      lastProcessingDuration = RUN_LOOP_SLEEP_MS + (RUN_LOOP_SINE_COEFF * Math.sin(Date.now() * RUN_LOOP_SINE_ANGLE_COEFF));
      console.log('Processing duration', lastProcessingDuration);
      await sleep(lastProcessingDuration);
    } catch (err) {
      onErr(err);
    }
  }
}

async function close() {
  console.log('Closing');
  await Promise.allSettled([
    publisher.channel.close(),
    consumer.channel.close(),
  ]);

  console.log('Channels closed');

  await Promise.allSettled([
    publisher.connection.close(),
    consumer.connection.close(),
  ]);
  console.log('Connections closed');
}

// main entrypoint

(async () => {
  const args = yargs
    .option('purge-queue', {
      alias: ['purge', 'p'],
      type: 'boolean',
      default: false,
    })
    .help()
    .parse();

  shouldPurgeQueue = args['purge-queue'];

  const shutdown = (err) => {
    if (err) {
      console.error(err);
    }
    isRunning = false;
    publisher.isRunning = false;
  };
  process.once('SIGINT', e => shutdown(e));

  await open();
  await runLoop(e => shutdown(e));
  await close();
})();
