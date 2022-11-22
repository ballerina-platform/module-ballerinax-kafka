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
# + consumerConfig - Stores configurations related to a Kafka connection
public isolated client class Listener {

    final ConsumerConfiguration & readonly consumerConfig;
    private final string keyDeserializerType;
    private final string valueDeserializerType;
    private final string|string[] & readonly bootstrapServers;

    # Creates a new `kafka:Listener`.
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

        string|string[]? topics = config?.topics;
        if topics is string|string[] {
            if self.consumerConfig?.groupId !is string {
                panic createError("The groupId of the consumer must be set to subscribe to the topics");
            }
        }
    }

    private isolated function listenerInit() returns Error? =
    @java:Method {
        name: "connect",
        'class: "io.ballerina.stdlib.kafka.nativeimpl.consumer.BrokerConnection"
    } external;

    # Starts the registered services.
    # ```ballerina
    # error? result = listener.'start();
    # ```
    #
    # + return - A `kafka:Error` if an error is encountered while starting the server or else `()`
    public isolated function 'start() returns error? =
    @java:Method {
        name: "start",
        'class: "io.ballerina.stdlib.kafka.service.Start"
    } external;

    # Stops the Kafka listener gracefully.
    # ```ballerina
    # error? result = listener.gracefulStop();
    # ```
    #
    # + return - A `kafka:Error` if an error is encountered during the listener-stopping process or else `()`
    public isolated function gracefulStop() returns error?  =
    @java:Method {
        'class: "io.ballerina.stdlib.kafka.service.Stop"
    } external;

    # Stops the kafka listener immediately.
    # ```ballerina
    # error? result = listener.immediateStop();
    # ```
    #
    # + return - A `kafka:Error` if an error is encountered during the listener-stopping process or else `()`
    public isolated function immediateStop() returns error? =
    @java:Method {
        'class: "io.ballerina.stdlib.kafka.service.Stop"
    } external;

    # Attaches a service to the listener.
    # ```ballerina
    # error? result = listener.attach(kafkaService);
    # ```
    #
    # + 'service - The service to be attached
    # + name - Name of the service
    # + return - A `kafka:Error` if an error is encountered while attaching the service or else `()`
    public isolated function attach(Service 'service, string[]|string? name = ()) returns error? =
    @java:Method {
        name: "register",
        'class: "io.ballerina.stdlib.kafka.service.Register"
    } external;

    # Detaches a consumer service from the listener.
    # ```ballerina
    # error? result = listener.detach(kafkaService);
    # ```
    #
    # + 'service - The service to be detached
    # + return - A `kafka:Error` if an error is encountered while detaching a service or else `()`
    public isolated function detach(Service 'service) returns error? =
    @java:Method {
        name: "unregister",
        'class: "io.ballerina.stdlib.kafka.service.Unregister"
    } external;
}
