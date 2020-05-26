const { URL } = require('url');
const amqplib = require('amqplib');
const fetch = require('node-fetch');

const host = 'localhost';
const port = 5672;
const mgmtPort = 15672;
const username = 'guest';
const password = 'guest';

const QUEUE_NAME = 'testQueue';

const PUBLISH_SLEEP_MS = 0;
// let the queue fill up a bit before reading
const PUBLISHER_HEAD_START_MS = 100;

const RUN_LOOP_SLEEP_MS = 0;
const RUN_LOOP_CHECKPOINT_INTERVAL = 10;

const CONSUMER_MAX_BUFFER = 50;
const PREFETCH_RATIO = 1.5;

const STATS_LOOP_SLEEP_MS = 500;

/** @type {Publisher} */
let publisher;
/** @type {Consumer} */
let consumer;
/** @type {Set<string>} */
let sessionIds;
/** @type {Array} */
let sessionMessages;

let isRunning = true;

const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

const round2 = (num) => Math.round((num + Number.EPSILON) * 100) / 100;

function getConnectionURI() {
  const url = new URL(`amqp://${host}`);
  url.username = username;
  url.password = password;
  url.port = `${port}`;
  return url.toString();
}

// set up a queue with same defaults as RMQ connector
async function declareQueue(channel) {
  await channel.assertQueue(QUEUE_NAME, {
    durable: true,
    exclusive: false,
    autoDelete: false,
    arguments: null,
  });
  // just in case there are lingerers from a previous run
  await channel.purgeQueue(QUEUE_NAME);
}

async function statsLoop() {
  while (isRunning) {
    const mgmtUrl = new URL(`http://${host}:${mgmtPort}/api/queues/${encodeURIComponent('/')}/${QUEUE_NAME}`);
    mgmtUrl.username = username;
    mgmtUrl.password = password;
    const res = await fetch(mgmtUrl);
    const queue = await res.json();
    const parallelism = 1;
    const approxUnacked = parallelism * (consumer.buffer.length + sessionIds.size);
    console.debug('stats', JSON.stringify({
      queue: {
        unacked: queue.messages_unacknowledged,
        ready: queue.messages_ready,
        'publish/s': queue.message_stats && queue.message_stats.publish_details.rate,
        'deliver/s': queue.message_stats && queue.message_stats.deliver_details.rate,
      },
      consumer: {
        'buffer length': consumer.buffer.length,
        prefetch: consumer.prefetchAmount,
      },
      session: {
        count: sessionIds.size,
      },
      approxUnacked,
      approxUnackedDelta: queue.messages_unacknowledged - approxUnacked,
      'Approx. Unacked % error': round2((queue.messages_unacknowledged - approxUnacked) / queue.messages_unacknowledged * 100),
    }));
    await sleep(STATS_LOOP_SLEEP_MS);
  }
}

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

class Consumer {
  // need to simulate backpressure and buffering messages
  // need to set prefetch in scale with the message buffer
  // pull as many messages as possible until buffer is full, then start backpressure
  // - need control to start and stop connection limiting
  // - Flink RateLimiter?

  // need to allow one message to be pulled at a time from the buffer

  // total messages delivered ~= parallelism * (# messages in consumer buffer + # messages in session ID buffer)
  //   - also, all messages filtered from session ID buffer but not acked?
  constructor(connection, channel) {
    this.buffer = [];
    this.maxBufferLength = CONSUMER_MAX_BUFFER;
    this.connection = connection;
    this.channel = channel;
    this.maxPrefetchAmount = CONSUMER_MAX_BUFFER * PREFETCH_RATIO;
    // if there is still room in the buffer
    // allow as many as possible in
    // NOTE: need to handle v3.3.0 global semantics: https://www.rabbitmq.com/consumer-prefetch.html
    this.prefetchAmount = false;
    this.channel.prefetch(this.prefetchAmount);
  }

  consume() {
    this.channel.consume(QUEUE_NAME, (msg) => {
      this.buffer.push(msg);
      this.configureQos();
    });
  }

  configureQos() {
    const curAmount = this.prefetchAmount;
    // console.debug('Consumer buffer length', this.buffer.length);
    if (this.buffer.length >= this.maxBufferLength) {
      // TODO: backpressure here using qos prefetch
      //    as determined by length of session IDs and current buffer (if buffer is "full")
      // console.debug('Consumer approx unacked', approxUnacked);
      // cap it at the current unacked until there is room for more
      this.prefetchAmount = this.maxPrefetchAmount;
    } else {
      this.prefetchAmount = false;
    }
    if (curAmount !== this.prefetchAmount) {
      this.channel.prefetch(this.prefetchAmount);
    }
  }

  getNextMessage() {
    // lifo queue
    return this.buffer.shift();
  }
}

// one of these will need to monitor the queue for stats
//    - # messages in queue ready/ delivered/ acked
//    - # messages delivered/s
//    - # messages published/s

// open the connections to Rabbit for both a publisher and a consumer
async function open() {
  sessionIds = new Set();
  sessionMessages = [];

  const pubConn = await amqplib.connect(getConnectionURI());
  const pubChan = await pubConn.createChannel();
  publisher = new Publisher(pubConn, pubChan);

  const consConn = await amqplib.connect(getConnectionURI());
  const consChan = await consConn.createChannel();
  consumer = new Consumer(consConn, consChan);

  await declareQueue(consChan);
}

// The main run loop, as in the RMQ source
async function run() {
  // on each run loop, do a quick calculation
  // and set the proper QOS in case of backpressure
  consumer.configureQos();

  // pull off the next message, add to checkpoint buffer
  // ack all message in buffer every x seconds
  const msg = consumer.getNextMessage();
  if (!msg) {
    // no messages ready
    console.log(`no available messages`);
    return;
  }
  // deliveryTag = envelope.deliveryTag
  // corrId = deliveryTag.props.corrId
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

  let runNum = 0;
  while (isRunning) {
    runNum += 1;
    try {
      console.log(`running ${runNum}`);
      await run();
      await sleep(RUN_LOOP_SLEEP_MS);
      if (runNum % RUN_LOOP_CHECKPOINT_INTERVAL === 0) {
        console.log(`checkpointing run ${runNum}`);
        await ackSessionIds();
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

(async () => {
  const onErr = (err) => {
    console.error(err);
    isRunning = false;
    publisher.isRunning = false;
  };
  await open();
  await runLoop(onErr);
  await close().then(console.log);
})();
