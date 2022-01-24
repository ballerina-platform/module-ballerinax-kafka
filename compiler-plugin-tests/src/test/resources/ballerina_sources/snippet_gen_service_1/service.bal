import ballerinax/kafka;

service kafka:Service on new kafka:Listener("localhost:9090") {}
