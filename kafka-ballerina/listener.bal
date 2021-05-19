
// Copyright (c) 2019 WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
//
// WSO2 Inc. licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except
// in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

import ballerina/jballerina.java;

# Represents a Kafka consumer endpoint.
#
# + consumerConfig - Used to store configurations related to a Kafka connection
public client class Listener {

    public ConsumerConfiguration consumerConfig;
    private string keyDeserializerType;
    private string valueDeserializerType;
    private string|string[] bootstrapServers;

    # Creates a new Kafka `Listener`.
    #
    # + bootstrapServers - List of remote server endpoints of kafka brokers
    # + config - Configurations related to consumer endpoint
    # + return - A `kafka:Error` if an error is encountered or else '()'
    public isolated function init (string|string[] bootstrapServers, *ConsumerConfiguration config) returns Error? {
        self.bootstrapServers = bootstrapServers;
        self.consumerConfig = config;
        self.keyDeserializerType = DES_BYTE_ARRAY;
        self.valueDeserializerType = DES_BYTE_ARRAY;
        check connect(self);

        string[]? topics = config?.topics;
        if (topics is string[]){
            check self.subscribe(topics);
        }
    }

    # Starts the registered services.
    #
    # + return - An `kafka:Error` if an error is encountered while starting the server or else nil
    public isolated function 'start() returns error? =
    @java:Method {
     'class: "org.ballerinalang.messaging.kafka.service.Start"
    } external;

    # Stops the kafka listener.
    #
    # + return - An `kafka:Error` if an error is encountered during the listener stopping process or else nil
    public isolated function gracefulStop() returns error? =
    @java:Method {
        name: "stop",
       'class: "org.ballerinalang.messaging.kafka.service.Stop"
    } external;

    # Stops the kafka listener.
    #
    # + return - An `kafka:Error` if an error is encountered during the listener stopping process or else nil
    public isolated function immediateStop() returns error? =
    @java:Method {
        name: "stop",
        'class: "org.ballerinalang.messaging.kafka.service.Stop"
    } external;

    # Gets called every time a service attaches itself to the listener.
    #
    # + s - The service to be attached
    # + name - Name of the service
    # + return - An `kafka:Error` if an error is encountered while attaching the service or else nil
    public isolated function attach(Service s, string[]|string? name = ()) returns error? =
    @java:Method {
        name: "register",
        'class: "org.ballerinalang.messaging.kafka.service.Register"
    } external;

    # Detaches a consumer service from the listener.
    #
    # + s - The service to be detached
    # + return - An `kafka:Error` if an error is encountered while detaching a service or else nil
    public isolated function detach(Service s) returns error? {
        // not implemented
    }

    private isolated function subscribe(string[] topics) returns Error? {
        if (self.consumerConfig?.groupId is string) {
            return consumerSubscribe(self, topics);
        } else {
            panic createError("The groupId of the consumer must be set to subscribe to the topics");
        }
    }
}
