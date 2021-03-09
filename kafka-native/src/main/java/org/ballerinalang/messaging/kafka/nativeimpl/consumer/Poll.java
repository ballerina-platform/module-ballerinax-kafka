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

package org.ballerinalang.messaging.kafka.nativeimpl.consumer;

import io.ballerina.runtime.api.Environment;
import io.ballerina.runtime.api.Future;
import io.ballerina.runtime.api.creators.TypeCreator;
import io.ballerina.runtime.api.creators.ValueCreator;
import io.ballerina.runtime.api.values.BArray;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.api.values.BString;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaException;
import org.ballerinalang.messaging.kafka.observability.KafkaMetricsUtil;
import org.ballerinalang.messaging.kafka.observability.KafkaObservabilityConstants;
import org.ballerinalang.messaging.kafka.observability.KafkaTracingUtil;
import org.ballerinalang.messaging.kafka.utils.KafkaConstants;

import java.time.Duration;

import static org.ballerinalang.messaging.kafka.utils.KafkaConstants.NATIVE_CONSUMER;
import static org.ballerinalang.messaging.kafka.utils.KafkaUtils.createKafkaError;
import static org.ballerinalang.messaging.kafka.utils.KafkaUtils.getConsumerRecord;
import static org.ballerinalang.messaging.kafka.utils.KafkaUtils.populateConsumerRecord;

/**
 * Native function polls the broker to retrieve messages within given timeout.
 */
public class Poll {

    /**
     * Polls from kafka broker using the ballerina kafka consumer.
     *
     * @param consumerObject Kafka consumer object from ballerina.
     * @param timeout        Duration in milliseconds to try the operation.
     * @return Ballerina {@code ConsumerRecords[]} after the polling.
     */
    public static Object poll(Environment env, BObject consumerObject, long timeout) {
        KafkaTracingUtil.traceResourceInvocation(env, consumerObject);
        Future balFuture = env.markAsync();
        KafkaConsumer kafkaConsumer = (KafkaConsumer) consumerObject.getNativeData(NATIVE_CONSUMER);
        String keyType = KafkaConstants.DEFAULT_SER_DES_TYPE;
        String valueType = KafkaConstants.DEFAULT_SER_DES_TYPE;
        Duration duration = Duration.ofMillis(timeout);
        BArray consumerRecordsArray = ValueCreator.createArrayValue(
                TypeCreator.createArrayType(getConsumerRecord().getType()));
        try {
            ConsumerRecords recordsRetrieved = kafkaConsumer.poll(duration);
            if (!recordsRetrieved.isEmpty()) {
                for (Object record : recordsRetrieved) {
                    BMap<BString, Object> recordValue = populateConsumerRecord((ConsumerRecord) record, keyType,
                                                                                   valueType);
                    consumerRecordsArray.append(recordValue);
                }
            }
            balFuture.complete(consumerRecordsArray);
        } catch (IllegalStateException | IllegalArgumentException | KafkaException e) {
            KafkaMetricsUtil.reportConsumerError(consumerObject, KafkaObservabilityConstants.ERROR_TYPE_POLL);
            balFuture.complete(createKafkaError("Failed to poll from the Kafka server: " + e.getMessage()));
        }
        return null;
    }
}
