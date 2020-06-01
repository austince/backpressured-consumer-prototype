# Backpressure-aware Consumer Prototype

For [FLINK-10195](https://issues.apache.org/jira/browse/FLINK-10195?jql=project%20%3D%20FLINK%20AND%20status%20%3D%20Open%20AND%20component%20%3D%20%22Connectors%2F%20RabbitMQ%22)

## Replacing the QueuingConsumer

Buffering the messages seems to still be a good approach, so messages can be pulled one-by-one
in the source read loop to keep the implementation there simple. We just need a way control
the flow of messages from the RMQ server.

## [Current PR (#8111)](https://github.com/apache/flink/pull/8111)

Main issue with current proposed implementation is that it stops/ starts the consumer when too
much data is sent to the client/ under backpressure. This has shown to be too costly
for production use, at around  ~60 messages/s.

The queue length mechanism works well and is used in other connectors (ex: kinesis).

## Proposed Changes

Instead of starting/ stopping, we can adjust the `prefetch` (see: [1] and [2]) amount on the RMQ channel, which
is the only way RMQ allows client-side flow control. 

With this prototype, the rough metrics seem to be very performant and keep up with the publishing. When the consumer
is throttled, the publish rate is set to be close to what can be consumed, and picks ups quickly.


### Running

```bash
$ docker-compose up &
$ node index.js | grep stats
# ...
stats {"ts":"2020-06-01T00:54:19.327Z","queue":{"unacked":69,"ready":0,"publish/s":854.4,"deliver/s":854.4},"consumer":{"buffer length":62,"prefetch":75},"session":{"count":9},"approxUnacked":{"estimate":71,"delta":-2,"% error":-2.9}}
stats {"ts":"2020-06-01T00:54:19.831Z","queue":{"unacked":69,"ready":0,"publish/s":854.4,"deliver/s":854.4},"consumer":{"buffer length":61,"prefetch":75},"session":{"count":9},"approxUnacked":{"estimate":70,"delta":-1,"% error":-1.45}}
stats {"ts":"2020-06-01T00:54:20.334Z","queue":{"unacked":69,"ready":0,"publish/s":854.4,"deliver/s":854.4},"consumer":{"buffer length":63,"prefetch":75},"session":{"count":5},"approxUnacked":{"estimate":68,"delta":1,"% error":1.45}}
```

## Potential Issues

* `global` prefetch
  * The meaning changes between versions of RMQ, but since the current `RMQSource` controls
    both the Connection and the Channel, we might be ok. The only case this isn't true is when
    users override one or both. `false` is the usual setting and will likely work for us.
  *  see: https://www.squaremobius.net/amqp.node/channel_api.html#channel_prefetch  
     _RabbitMQ v3.3.0 changes the meaning of prefetch (basic.qos) to apply per-consumer, rather than per-channel.
     It will apply to consumers started after the method is called. See rabbitmq-prefetch._
* Dynamically setting the `prefetch`
  * If the `prefetch` set at the creation needs to change, an example
    of changing this on the fly can be found here: https://github.com/MassTransit/MassTransit/blob/master/src/MassTransit.RabbitMqTransport.Tests/SetPrefetchCount_Specs.cs   

## Tickets addressed

- [FLINK-10195]
- [FLINK-6885] RMQSource does not support qos, leading to oom
  - we could allow this prefect mechanism to be configurable
- [FLINK-17559] Duplicate of `FLINK-6885`

[1]: https://www.rabbitmq.com/consumer-prefetch.html
[2]: https://www.rabbitmq.com/confirms.html
[FLINK-10195]: https://issues.apache.org/jira/browse/FLINK-10195
[FLINK-6885]: https://issues.apache.org/jira/browse/FLINK-6885
[FLINK-17559]: https://issues.apache.org/jira/browse/FLINK-17559
