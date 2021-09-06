import ballerina/test;
import ballerina/http;
import ballerinax/kafka;
import ballerina/lang.value;
import order_service.types;

@test:Config{}
function orderServiceTest() returns error? {
    http:Client orderClient = check new ("http://localhost:9090");

    string orderName = "PS5";
    string orderStatus = "SUCCESS";

    string response = check orderClient->get("/kafka/publish?message=PS5&status=SUCCESS");
    string expectedResponse = "Message sent to the Kafka topic " + TOPIC + " successfully. Order " + orderName
                + " with status " + orderStatus;
    test:assertEquals(response, expectedResponse);

    kafka:ConsumerConfiguration testConsumerConfigs = {
        groupId: "order-service-consumer",
        offsetReset: kafka:OFFSET_RESET_EARLIEST,
        topics: [TOPIC]
    };

    kafka:Consumer testConsumer = check new (kafka:DEFAULT_URL, testConsumerConfigs);
    kafka:ConsumerRecord[] records = check testConsumer->poll(3);

    test:assertEquals(records.length(), 1);

    string messageContent = check string:fromBytes(records[0].value);
    json jsonContent = check value:fromJsonString(messageContent);
    json jsonClone = jsonContent.cloneReadOnly();
    types:Order neworder = <types:Order> jsonClone;

    test:assertEquals(neworder.name, orderName);
    test:assertEquals(neworder.status, orderStatus);
}
