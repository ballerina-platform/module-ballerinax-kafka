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

# Represents a Kafka caller, which can be used to commit the
# offsets consumed by the service.
public client class Caller {

    # Commits the current consumed offsets for the service.
    # ```ballerina
    # kafka:Error? result = caller->commit();
    # ```
    #
    # + return - A `kafka:Error` if an error is encountered or else '()'
    isolated remote function 'commit() returns Error? {
        return consumerCommit(self);
    }

    # Commits given offsets and partitions for the given topics, for service.
    #
    # + offsets - Offsets to be commited
    # + duration - Timeout duration for the commit operation execution
    # + return - `kafka:Error` if an error is encountered or else nil
    isolated remote function commitOffset(PartitionOffset[] offsets, int duration = -1) returns Error? {
        return consumerCommitOffset(self, offsets, duration);
    }
}


