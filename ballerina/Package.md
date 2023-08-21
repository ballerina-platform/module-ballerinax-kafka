## Overview
This package provides an implementation to interact with Kafka Brokers via Kafka Consumer and Kafka Producer clients.

Apache Kafka is an open-source distributed event streaming platform used for high-performance data pipelines, streaming analytics, data integration, and mission-critical applications.

This package supports Kafka 1.x.x and 2.0.0 versions.

### Consumer and producer
#### Kafka producer
A Kafka producer is a Kafka client that publishes records to the Kafka cluster. The producer is thread-safe and sharing a single producer instance across threads will generally be faster than having multiple instances. When working with a Kafka producer, the first thing to do is to initialize the producer.
For the producer to execute successfully, an active Kafka broker should be available.

The code snippet given below initializes a producer with the basic configuration.
```ballerina
import ballerinax/kafka;

kafka:ProducerConfiguration producerConfiguration = {
    clientId: "basic-producer",
    acks: "all",
    retryCount: 3
};

kafka:Producer kafkaProducer = check new (kafka:DEFAULT_URL, producerConfiguration);
```
#### Kafka consumer
A Kafka consumer is a subscriber responsible for reading records from one or more topics and one or more partitions of a topic. When working with a Kafka consumer, the first thing to do is initialize the consumer.
For the consumer to execute successfully, an active Kafka broker should be available.

The code snippet given below initializes a consumer with the basic configuration.
```ballerina
kafka:ConsumerConfiguration consumerConfiguration = {
    groupId: "group-id",    // Unique string that identifies the consumer
    offsetReset: "earliest",    // Offset reset strategy if no initial offset
    topics: ["kafka-topic"]
};

kafka:Consumer kafkaConsumer = check new (kafka:DEFAULT_URL, consumerConfiguration);
```
### Listener
The Kafka consumer can be used as a listener to a set of topics without the need to manually `poll` the messages.

You can use the `Caller` to manually commit the offsets of the messages that are read by the service. The following code snippet shows how to initialize and define the listener and how to commit the offsets manually.
```ballerina
kafka:ConsumerConfiguration consumerConfiguration = {
    groupId: "group-id",
    topics: ["kafka-topic-1"],
    pollingInterval: 1,
    autoCommit: false
};

listener kafka:Listener kafkaListener = new (kafka:DEFAULT_URL, consumerConfiguration);

service on kafkaListener {
    remote function onConsumerRecord(kafka:Caller caller, kafka:ConsumerRecord[] records) {
        // processes the records
        ...
        // commits the offsets manually
        kafka:Error? commitResult = caller->commit();

        if commitResult is kafka:Error {
            log:printError("Error occurred while committing the offsets for the consumer ", 'error = commitResult);
        }
    }
}
```
### Data serialization
Serialization is the process of converting data into a stream of bytes that is used for transmission. Kafka
stores and transmits these bytes of arrays in its queue. Deserialization does the opposite of serialization
in which bytes of arrays are converted into the desired data type.

Currently, this package only supports the `byte array` data type for both the keys and values. The following code snippets
show how to produce and read a message from Kafka.
```ballerina
string message = "Hello World, Ballerina";
string key = "my-key";
// converts the message and key to a byte array
check kafkaProducer->send({ topic: "test-kafka-topic", key: key.toBytes(), value: message.toBytes() });
```
```ballerina
kafka:ConsumerRecord[] records = check kafkaConsumer->poll(1);

foreach var kafkaRecord in records {
    byte[] messageContent = kafkaRecord.value;
    // tries to generate the string value from the byte array
    string result = check string:fromBytes(messageContent);
    io:println("The result is : ", result);
}
```
### Concurrency
In Kafka, records are grouped into smaller units called partitions. These can be processed independently without
compromising the correctness of the results and lays the foundation for parallel processing. This can be achieved by
using multiple consumers within the same group each reading and processing data from a subset of topic partitions and
running in a single thread.

Topic partitions are assigned to consumers automatically or you can manually assign topic partitions.

The following code snippet joins a consumer to the `consumer-group` and assigns it to a topic partition manually.
```ballerina
kafka:ConsumerConfiguration consumerConfiguration = {
    // `groupId` determines the consumer group
    groupId: "consumer-group",
    pollingInterval: 1,
    autoCommit: false
};

kafka:Consumer kafkaConsumer = check new (kafka:DEFAULT_URL, consumerConfiguration);
// creates a topic partition
kafka:TopicPartition topicPartition = {
    topic: "kafka-topic-1",
    partition: 1
};
// passes the topic partitions to the assign function as an array
check kafkaConsumer->assign([topicPartition]);
```

### Report issues

To report bugs, request new features, start new discussions, view project boards, etc., go to the [Ballerina standard library parent repository](https://github.com/ballerina-platform/ballerina-standard-library).

### Useful links

- Chat live with us via our [Discord server](https://discord.gg/ballerinalang).
- Post all technical questions on Stack Overflow with the [#ballerina](https://stackoverflow.com/questions/tagged/ballerina) tag.
