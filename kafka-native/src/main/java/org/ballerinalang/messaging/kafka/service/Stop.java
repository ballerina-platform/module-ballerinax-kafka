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

package org.ballerinalang.messaging.kafka.service;

import io.ballerina.runtime.api.values.BObject;
import org.ballerinalang.messaging.kafka.exceptions.KafkaConnectorException;
import org.ballerinalang.messaging.kafka.impl.KafkaServerConnectorImpl;
import org.ballerinalang.messaging.kafka.utils.KafkaUtils;

import java.io.PrintStream;

import static org.ballerinalang.messaging.kafka.utils.KafkaConstants.SERVER_CONNECTOR;
import static org.ballerinalang.messaging.kafka.utils.KafkaConstants.SERVICE_STOPPED;
import static org.ballerinalang.messaging.kafka.utils.KafkaUtils.getBrokerNames;

/**
 * Stop the server connector.
 */
public class Stop {
    private static final PrintStream console = System.out;

    public static Object stop(BObject listener) {
        KafkaServerConnectorImpl serverConnector = (KafkaServerConnectorImpl) listener.getNativeData(SERVER_CONNECTOR);
        boolean isStopped;
        try {
            isStopped = serverConnector.stop();
        } catch (KafkaConnectorException e) {
            return KafkaUtils.createKafkaError(e.getMessage());
        }
        if (!isStopped) {
            return KafkaUtils.createKafkaError("Failed to stop the kafka service.");
        }
        console.println(SERVICE_STOPPED + getBrokerNames(listener));
        return null;
    }
}
