// Copyright (c) 2020 WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

# Represents a Kafka caller, which can be used to commit the offsets consumed by the service.
public client isolated class Caller {

    # Commits the currently consumed offsets of the service.
    # ```ballerina
    # kafka:Error? result = caller->commit();
    # ```
    #
    # + return - A `kafka:Error` if an error is encountered or else '()'
    isolated remote function 'commit() returns Error? =
    @java:Method {
        name: "commit",
        'class: "io.ballerina.stdlib.kafka.nativeimpl.consumer.Commit"
    } external;

    # Commits the given offsets and partitions for the given topics of the service.
    # ```ballerina
    # kafka:Error? result = caller->commitOffset([partitionOffset1, partitionOffset2]);
    # ```
    #
    # + offsets - Offsets to be commited
    # + duration - Timeout duration (in seconds) for the commit operation execution
    # + return - A `kafka:Error` if an error is encountered or else `()`
    isolated remote function commitOffset(PartitionOffset[] offsets, decimal duration = -1) returns Error? =
    @java:Method {
        name: "commitOffset",
        'class: "io.ballerina.stdlib.kafka.nativeimpl.consumer.Commit"
    } external;
}


