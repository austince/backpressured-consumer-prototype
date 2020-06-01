# Backpressure-aware Consumer Prototype

For [FLINK-10195], a simple RabbitMQ queued consumer implementation that sets a prefetch to the buffer size
and simulates checkpointing. Backpressure is simulated by sleeping after pulling a message on a periodic function
that dips between being faster and slower than the producer.

## [Current PR (#8111)](https://github.com/apache/flink/pull/8111)

Main issue with current proposed implementation is that it stops/ starts the consumer when too
much data is sent to the client/ under backpressure. This has shown to be too costly
for production use, at around  ~60 messages/s.

The queue length mechanism works well and is used in other connectors (ex: kinesis).
Messages can be pulled one-by-one from the source read loop to keep the implementation there simple.
We just need a way control the flow of messages from the RMQ server.

## Proposed Changes

__tldr;__
Set `prefetch` count (aka `basicQos`) to be the size of a configurable buffer.


Instead of starting/ stopping, we can adjust the `prefetch` (see: [1] and [2]) amount on the RMQ channel, which
is the only way RMQ allows client-side flow control. 

With this prototype, the rough metrics seem to be performant and keep up with the publishing. When the consumer
is throttled (i.e. the unacked count == prefetch limit), the publish rate is set to be close to what can be consumed,
and picks ups quickly as soon as there is space in the buffer.

### Running

```bash
$ docker-compose up &
$ node index.js --purge | grep stats
# ...
stats {"ts":"2020-06-01T17:37:17.679Z","queue":{"unacked":322,"ready":0,"publish/s":54.4,"deliver/s":99.6,"delta/s":-45.2},"consumer":{"processing dur":19.97,"buffer length":68,"buffer space":432,"prefetch":500,"prefetch space":178},"session":{"session length":432},"unacked":{"estimate":500,"delta":-178,"% error":-55.28}}
stats {"ts":"2020-06-01T17:37:18.190Z","queue":{"unacked":322,"ready":0,"publish/s":54.4,"deliver/s":99.6,"delta/s":-45.2},"consumer":{"processing dur":16.06,"buffer length":43,"buffer space":457,"prefetch":500,"prefetch space":178},"session":{"session length":457},"unacked":{"estimate":500,"delta":-178,"% error":-55.28}}
stats {"ts":"2020-06-01T17:37:18.701Z","queue":{"unacked":500,"ready":95,"publish/s":54.6,"deliver/s":0.4,"delta/s":54.2},"consumer":{"processing dur":24.29,"buffer length":11,"buffer space":489,"prefetch":500,"prefetch space":0},"session":{"session length":489},"unacked":{"estimate":500,"delta":0,"% error":0}}
```

### Potential Issues

* `global` prefetch
  * The meaning changes between versions of RMQ, but since the current `RMQSource` controls
    both the Connection and the Channel, we might be ok.
  * The only case this isn't true is when users override one or both.
    `false` is the usual setting and will likely work for us.
  * see: https://www.squaremobius.net/amqp.node/channel_api.html#channel_prefetch  
     _RabbitMQ v3.3.0 changes the meaning of prefetch (basic.qos) to apply per-consumer, rather than per-channel.
     It will apply to consumers started after the method is called. See rabbitmq-prefetch._
* Limiting-by-channel
  * A Channel can be used to read from many queues, so setting a `prefetch` has to be synced
    across each queue on the server, which is slow on clusters.
  * Like above, since the `RMQSource` currently controls the Channel and only reads from
    a single queue. Will only be a problem if users override poorly.
* Dynamically setting the `prefetch`
  * If the `prefetch` set at the creation needs to change, an example project that
    changes this on the fly can be found here: https://github.com/MassTransit/MassTransit/blob/master/src/MassTransit.RabbitMqTransport.Tests/SetPrefetchCount_Specs.cs   

## Tickets addressed

- [FLINK-10195]
- [FLINK-6885] RMQSource does not support qos, leading to oom
  - we should allow this prefetch/ max buffer length mechanism to be configurable
- [FLINK-17559] Duplicate of `FLINK-6885`

[1]: https://www.rabbitmq.com/consumer-prefetch.html
[2]: https://www.rabbitmq.com/confirms.html
[FLINK-10195]: https://issues.apache.org/jira/browse/FLINK-10195
[FLINK-6885]: https://issues.apache.org/jira/browse/FLINK-6885
[FLINK-17559]: https://issues.apache.org/jira/browse/FLINK-17559
