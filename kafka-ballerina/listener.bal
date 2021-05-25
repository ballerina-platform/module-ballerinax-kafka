
// Copyright (c) 2021 WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
public isolated client class Listener {

    final ConsumerConfiguration & readonly consumerConfig;
    private final string keyDeserializerType;
    private final string valueDeserializerType;
    private final string|string[] & readonly bootstrapServers;

    # Creates a new Kafka `Listener`.
    #
    # + bootstrapServers - List of remote server endpoints of Kafka brokers
    # + config - Configurations related to the consumer endpoint
    # + return - A `kafka:Error` if an error is encountered or else '()'
    public isolated function init (string|string[] bootstrapServers, *ConsumerConfiguration config) returns Error? {
        self.bootstrapServers = bootstrapServers.cloneReadOnly();
        self.consumerConfig = config.cloneReadOnly();
        self.keyDeserializerType = DES_BYTE_ARRAY;
        self.valueDeserializerType = DES_BYTE_ARRAY;
        check self.listenerInit();

        string[]? topics = config?.topics;
        if (topics is string[]){
            if (self.consumerConfig?.groupId is string) {
                check self->consumerSubscribe(topics);
            } else {
                panic createError("The groupId of the consumer must be set to subscribe to the topics");
            }
        }
    }

    private isolated function listenerInit() returns Error? =
    @java:Method {
        name: "connect",
        'class: "org.ballerinalang.messaging.kafka.nativeimpl.consumer.BrokerConnection"
    } external;

    # Starts the registered services.
    #
    # + return - An `kafka:Error` if an error is encountered while starting the server or else nil
    public isolated function 'start() returns error? =
    @java:Method {
        name: "start",
        'class: "org.ballerinalang.messaging.kafka.service.Start"
    } external;

    # Stops the Kafka listener gracefully.
    #
    # + return - A `kafka:Error` if an error is encountered during the listener-stopping process or else `()`
    public isolated function gracefulStop() returns error?  =
    @java:Method {
        name: "stop",
        'class: "org.ballerinalang.messaging.kafka.service.Stop"
    } external;

    # Stops the kafka listener immediately.
    #
    # + return - A `kafka:Error` if an error is encountered during the listener-stopping process or else `()`
    public isolated function immediateStop() returns error? =
    @java:Method {
        name: "stop",
        'class: "org.ballerinalang.messaging.kafka.service.Stop"
    } external;

    # Attaches a service to the listener.
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
    # + return - A `kafka:Error` if an error is encountered while detaching a service or else `()`
    public isolated function detach(Service s) returns error? {
        // not implemented
    }

    isolated remote function consumerSubscribe(string[] topics) returns Error? =
    @java:Method {
        name: "subscribe",
        'class: "org.ballerinalang.messaging.kafka.nativeimpl.consumer.SubscriptionHandler"
    } external;
}
