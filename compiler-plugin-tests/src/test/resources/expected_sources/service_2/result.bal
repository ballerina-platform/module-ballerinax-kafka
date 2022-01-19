import ballerinax/kafka;

service kafka:Service on new kafka:Listener("localhost:9090") {
    int x = 5;
    string y = "xx";
    remote function onConsumerRecord(kafka:Caller caller, kafka:ConsumerRecord[] records) returns kafka:Error? {

    }
}
