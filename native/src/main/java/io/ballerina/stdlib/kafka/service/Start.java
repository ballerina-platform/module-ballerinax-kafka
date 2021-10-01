/*
 * Copyright (c) 2019, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.ballerina.stdlib.kafka.service;

import io.ballerina.runtime.api.values.BObject;
import io.ballerina.stdlib.kafka.exceptions.KafkaConnectorException;
import io.ballerina.stdlib.kafka.impl.KafkaServerConnectorImpl;

import java.io.PrintStream;
import java.util.List;

import static io.ballerina.stdlib.kafka.utils.KafkaConstants.SERVER_CONNECTOR;
import static io.ballerina.stdlib.kafka.utils.KafkaConstants.SERVICE_STARTED;
import static io.ballerina.stdlib.kafka.utils.KafkaUtils.createKafkaError;
import static io.ballerina.stdlib.kafka.utils.KafkaUtils.getTopicNamesString;

/**
 * Start server connector.
 */
public class Start {
    private static final PrintStream console = System.out;

    public static Object start(BObject listener) {
        KafkaServerConnectorImpl serverConnector = (KafkaServerConnectorImpl) listener.getNativeData(SERVER_CONNECTOR);
        if (serverConnector == null || serverConnector.isStarted()) {
            return null;
        }
        try {
            serverConnector.start();
            console.println(SERVICE_STARTED + getTopicNamesString((List<String>) listener.getNativeData("topics")));
        } catch (KafkaConnectorException e) {
            return createKafkaError(e.getMessage());
        }
        return null;
    }
}
