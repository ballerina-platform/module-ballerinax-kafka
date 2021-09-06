import ballerina/test;
import order_processor.types;
import ballerinax/kafka;
import ballerina/lang.runtime;
import ballerina/lang.value;

@test:Config{}
function orderProcessorTest() returns error? {
    kafka:Producer testProducer = check new (kafka:DEFAULT_URL);

    types:Order 'order = {
        id: 1,
        name: "Test Order",
        status: types:SUCCESS
    };
    check kafkaProducer->send({ topic: LISTENING_TOPIC, value: 'order.toString().toBytes()});

    kafka:ConsumerConfiguration testConsumerConfigs = {
        groupId: "test-consumer",
        offsetReset: kafka:OFFSET_RESET_EARLIEST,
        topics: [PUBLISH_TOPIC]
    };

    runtime:sleep(4);

    kafka:Consumer testConsumer = check new (kafka:DEFAULT_URL, testConsumerConfigs);
    kafka:ConsumerRecord[] records = check testConsumer->poll(3);

    test:assertEquals(records.length(), 2);

    string messageContent = check string:fromBytes(records[1].value);
    json content = check value:fromJsonString(messageContent);
    json jsonTweet = content.cloneReadOnly();
    types:Order neworder = <types:Order> jsonTweet;

    test:assertEquals(neworder, 'order);
}