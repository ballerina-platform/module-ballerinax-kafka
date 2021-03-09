## Package Overview

This package is used to interact with Kafka Brokers via Kafka Consumer and Kafka Producer clients.
This package supports Kafka 1.x.x and 2.0.0 versions.

For information on the operations, which you can perform with this package, see the below **Functions**.
For examples on the usage of the operations, see the following. 
* [Producer Example](https://ballerina.io/learn/by-example/kafka-producer.html) 
* [Consumer Service Example](https://ballerina.io/learn/by-example/kafka-consumer-service.html)
* [Consumer Client Example](https://ballerina.io/learn/by-example/kafka-consumer-client.html)
* [Transactional Producer Example](https://ballerina.io/learn/by-example/kafka-producer-transactional.html)
* [Consumer with SASL Authentication Example](https://ballerina.io/learn/by-example/kafka-authentication-sasl-plain-consumer.html)
* [Producer with SASL Authentication Example](https://ballerina.io/learn/by-example/kafka-authentication-sasl-plain-producer.html)

#### Basic Usages

##### Publishing Messages

1. Initialize the Kafka message producer.
```ballerina
kafka:ProducerConfiguration producerConfiguration = {
    bootstrapServers: "localhost:9092",
    clientId: "basic-producer",
    acks: "all",
    retryCount: 3,
    valueSerializerType: kafka:SER_BYTE_ARRAY,
    keySerializerType: kafka:SER_BYTE_ARRAY
};

kafka:Producer kafkaProducer = new (producerConfiguration);
```
2. Use the `kafka:Producer` to publish messages. 
```ballerina
string message = "Hello World, Ballerina";
kafka:Error? result = kafkaProducer->send(message.toBytes(), "kafka-topic", key = 1);
```

##### Consuming Messages

1. Initializing the Kafka message consumer. 
```ballerina
kafka:ConsumerConfiguration consumerConfiguration = {
    bootstrapServers: "localhost:9092",
    groupId: "group-id",
    offsetReset: "earliest",
    topics: ["kafka-topic"]
};

kafka:Consumer consumer = new (consumerConfiguration);
```
2. Use the `kafka:Consumer` as a simple record consumer.
```ballerina
kafka:ConsumerRecord[]|kafka:Error result = consumer->poll(1000);
```
3. Use the `kafka:Consumer` as a listener.
```ballerina
listener kafka:Listener lis = new (consumerConfiguration);

service kafkaService on lis {
    // This resource will be executed when a message is published to the
    // subscribed topic/topics.
    remote function onConsumerRecord(kafka:Caler caller,
            kafka:ConsumerRecord[] records) {
    }
}
```
