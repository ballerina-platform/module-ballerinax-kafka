/*
 * Copyright (c) 2020, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package org.ballerinalang.messaging.kafka.serdes;

import io.ballerina.runtime.api.Runtime;
import io.ballerina.runtime.api.ValueCreator;
import io.ballerina.runtime.api.values.BArray;
import io.ballerina.runtime.api.values.BObject;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.ballerinalang.messaging.kafka.utils.KafkaConstants;
import org.ballerinalang.messaging.kafka.utils.KafkaUtils;

import java.util.Map;
import java.util.Objects;

import static org.ballerinalang.messaging.kafka.utils.KafkaConstants.BALLERINA_STRAND;
import static org.ballerinalang.messaging.kafka.utils.KafkaConstants.ON_CLOSE_METADATA;
import static org.ballerinalang.messaging.kafka.utils.KafkaConstants.ON_DESERIALIZE_METADATA;

/**
 * Represents a deserializer class for ballerina kafka module.
 */
public class BallerinaKafkaDeserializer implements Deserializer {

    private BObject deserializerObject = null;
    private Runtime runtime = null;
    private int timeout = 30000;

    @Override
    public void configure(Map configs, boolean isKey) {
        this.runtime = (Runtime) configs.get(BALLERINA_STRAND);
        if (isKey) {
            this.deserializerObject = (BObject) configs.get(KafkaConstants.CONSUMER_KEY_DESERIALIZER_CONFIG);
        } else {
            this.deserializerObject = (BObject) configs.get(KafkaConstants.CONSUMER_VALUE_DESERIALIZER_CONFIG);
        }
        if (Objects.nonNull(configs.get(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG))) {
            this.timeout = (int) configs.get(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG);
        }
    }

    @Override
    public Object deserialize(String topic, byte[] data) {
        BArray bData = ValueCreator.createArrayValue(data);
        Object[] args = new Object[]{bData, false};
        return KafkaUtils.invokeMethodSync(runtime, this.deserializerObject, KafkaConstants.FUNCTION_DESERIALIZE,
                                           null, ON_DESERIALIZE_METADATA, this.timeout, args);
    }

    @Override
    public void close() {
        KafkaUtils.invokeMethodSync(runtime, this.deserializerObject, KafkaConstants.FUNCTION_CLOSE,
                                    null, ON_CLOSE_METADATA, this.timeout);
    }

}
