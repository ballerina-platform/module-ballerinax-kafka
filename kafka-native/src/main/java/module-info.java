/*
 * Copyright (c) 2020 WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

module io.ballerina.stdlib.kafka {
    requires kafka.clients;
    requires io.ballerina.jvm;
    requires slf4j.api;
    requires java.transaction.xa;
    requires org.apache.avro;
    exports org.ballerinalang.messaging.kafka.impl;
    exports org.ballerinalang.messaging.kafka.nativeimpl.consumer;
    exports org.ballerinalang.messaging.kafka.nativeimpl.producer;
    exports org.ballerinalang.messaging.kafka.serdes;
    exports org.ballerinalang.messaging.kafka.service;
    exports org.ballerinalang.messaging.kafka.observability;
    exports org.ballerinalang.messaging.kafka.api;
    exports org.ballerinalang.messaging.kafka.exceptions;
}
