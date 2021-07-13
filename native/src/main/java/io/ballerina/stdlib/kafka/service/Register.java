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

import io.ballerina.runtime.api.Environment;
import io.ballerina.runtime.api.Runtime;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.api.values.BString;
import io.ballerina.stdlib.kafka.api.KafkaListener;
import io.ballerina.stdlib.kafka.api.KafkaServerConnector;
import io.ballerina.stdlib.kafka.exceptions.KafkaConnectorException;
import io.ballerina.stdlib.kafka.impl.KafkaListenerImpl;
import io.ballerina.stdlib.kafka.impl.KafkaServerConnectorImpl;
import io.ballerina.stdlib.kafka.utils.KafkaUtils;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Objects;
import java.util.Properties;

import static io.ballerina.stdlib.kafka.utils.KafkaConstants.CONSUMER_BOOTSTRAP_SERVERS_CONFIG;
import static io.ballerina.stdlib.kafka.utils.KafkaConstants.CONSUMER_CONFIG_FIELD_NAME;
import static io.ballerina.stdlib.kafka.utils.KafkaConstants.NATIVE_CONSUMER;
import static io.ballerina.stdlib.kafka.utils.KafkaConstants.SERVER_CONNECTOR;
import static io.ballerina.stdlib.kafka.utils.KafkaConstants.UNCHECKED;

/**
 * This is used to register a listener to the kafka service.
 */
public class Register {

    @SuppressWarnings(UNCHECKED)
    public static Object register(Environment env, BObject listener, BObject service, Object name) {
        Object bootStrapServer = listener.get(CONSUMER_BOOTSTRAP_SERVERS_CONFIG);
        BMap<BString, Object> listenerConfigurations = listener.getMapValue(CONSUMER_CONFIG_FIELD_NAME);
        Properties configs = KafkaUtils.processKafkaConsumerConfig(bootStrapServer, listenerConfigurations);
        Runtime runtime = env.getRuntime();

        try {
            KafkaConsumer kafkaConsumer = null;
            if (Objects.nonNull(listener.getNativeData(NATIVE_CONSUMER))) {
                kafkaConsumer = (KafkaConsumer) listener.getNativeData(NATIVE_CONSUMER);
            }
            KafkaListener kafkaListener = new KafkaListenerImpl(listener, service, runtime);
            String serviceId = service.getType().getQualifiedName();
            KafkaServerConnector serverConnector = new KafkaServerConnectorImpl(serviceId, configs, kafkaListener,
                    kafkaConsumer);
            listener.addNativeData(SERVER_CONNECTOR, serverConnector);
        } catch (KafkaConnectorException e) {
            return KafkaUtils.createKafkaError(e.getMessage());
        }
        return null;
    }
}
