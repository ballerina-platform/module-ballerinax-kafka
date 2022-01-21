import ballerinax/kafka;

service kafka:Service on new kafka:Listener("localhost:9090") {
	remote function onConsumerRecord(kafka:Caller caller, kafka:ConsumerRecord[] records) returns kafka:Error? {

	}
}
