## Module Overview

This module provides an implementation to interact with Kafka Brokers via Kafka Consumer and Kafka Producer clients.

Apache Kafka is an open-source distributed event streaming platform used for high-performance data pipelines, streaming analytics, data integration, and mission-critical applications.

This module supports Kafka 1.x.x and 2.0.0 versions.

### Kafka Producer
A Kafka producer is a Kafka client that publishes records to the Kafka cluster. The producer is thread-safe and sharing a single producer instance across threads will generally be faster than having multiple instances. When working with a Kafka producer, the first thing to do is initialize the producer.
For the producer to execute successfully, an active Kafka broker should be available.

#### Initialize the Kafka message producer
The code snippet given below initializes a producer with basic configuration.
```ballerina
import ballerinax/kafka;

kafka:ProducerConfiguration producerConfiguration = {
    clientId: "basic-producer",
    acks: "all",
    retryCount: 3
};

kafka:Producer kafkaProducer = check new (kafka:DEFAULT_URL, producerConfiguration);
```

#### Use the Kafka producer to send messages
To send a message to the Kafka cluster, a `topic` and a `value` must be specified and the `key` is optional.
```ballerina
string message = "Hello World, Ballerina";
string key = "my-key";
check kafkaProducer->send({ topic: "test-kafka-topic", key: key.toBytes(), value: message.toBytes() });
```

#### Flush the records sent to the producer
Use the `flush` function to flush the batch of records already sent to the broker by the producer.
```ballerina
check kafkaProducer->'flush();
```

[comment]: <> (#### Retrieve topic partition information of a particular topic)

[comment]: <> (Use the below code snippet to retrieve topic partition information of a topic. A `TopicPartition` is a record containing the `topic` and the index of the partition.)

[comment]: <> (```ballerina)

[comment]: <> (string topic = "kafka-topic";)

[comment]: <> (kafka:TopicPartition[] result = check producer->getTopicPartitions&#40;topic&#41;;)

[comment]: <> (```)

#### Close the producer connection with Kafka cluster
Use the `close` function to close the producer connection to the external Kafka cluster.
```ballerina
check producer->close();
```

### Kafka Consumer
A Kafka consumer is a subscriber responsible for reading records from one or more topics and one or more partitions of a topic. When working with a Kafka producer, the first thing to do is initialize the producer.
For the consumer to execute successfully, an active Kafka broker should be available.

#### Initialize the Kafka message consumer
The code snippet given below initializes a consumer with basic configuration.
```ballerina
kafka:ConsumerConfiguration consumerConfiguration = {
    groupId: "group-id",
    offsetReset: "earliest",
    topics: ["kafka-topic"]
};

kafka:Consumer kafkaConsumer = check new (kafka:DEFAULT_URL, consumerConfiguration);
```

### Use the Kafka consumer to read messages







For information on the operations, which you can perform with this package, see the below **Functions**.
For examples on the usage of the operations, see the following.
* [Producer Example](https://ballerina.io/learn/by-example/kafka-producer.html)
* [Consumer Service Example](https://ballerina.io/learn/by-example/kafka-consumer-service.html)
* [Consumer Client Example](https://ballerina.io/learn/by-example/kafka-consumer-client.html)
* [Transactional Producer Example](https://ballerina.io/learn/by-example/kafka-producer-transactional.html)
* [Consumer with SASL Authentication Example](https://ballerina.io/learn/by-example/kafka-authentication-sasl-plain-consumer.html)
* [Producer with SASL Authentication Example](https://ballerina.io/learn/by-example/kafka-authentication-sasl-plain-producer.html)


##### Consuming Messages
```
2. Use the `kafka:Consumer` as a simple record consumer.
```ballerina
kafka:ConsumerRecord[]|kafka:Error result = consumer->poll(1);
```
3. Use the `kafka:Listener` as a listener.
```ballerina
listener kafka:Listener kafkaListener = new (kafka:DEFAULT_URL, consumerConfiguration);

service kafka:Service on kafkaListener {
    remote function onConsumerRecord(kafka:Caller caller,
                                kafka:ConsumerRecord[] records) {
    }
}
```
