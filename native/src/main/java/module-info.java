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

module io.ballerina.stdlib.kafka.runtime {
    requires kafka.clients;
    requires io.ballerina.runtime;
    requires io.ballerina.lang.value;
    requires java.transaction.xa;
    requires org.slf4j;
    requires java.logging;
    requires io.ballerina.stdlib.constraint;
    exports io.ballerina.stdlib.kafka.impl;
    exports io.ballerina.stdlib.kafka.nativeimpl.consumer;
    exports io.ballerina.stdlib.kafka.nativeimpl.producer;
    exports io.ballerina.stdlib.kafka.service;
    exports io.ballerina.stdlib.kafka.observability;
    exports io.ballerina.stdlib.kafka.api;
    exports io.ballerina.stdlib.kafka.exceptions;
}
