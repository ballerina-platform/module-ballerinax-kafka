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

package io.ballerina.stdlib.kafka.nativeimpl.consumer;

import io.ballerina.runtime.api.Environment;
import io.ballerina.runtime.api.values.BArray;
import io.ballerina.runtime.api.values.BDecimal;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.stdlib.kafka.observability.KafkaMetricsUtil;
import io.ballerina.stdlib.kafka.observability.KafkaObservabilityConstants;
import io.ballerina.stdlib.kafka.observability.KafkaTracingUtil;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Map;
import java.util.Properties;

import static io.ballerina.stdlib.kafka.utils.KafkaConstants.ALIAS_DURATION;
import static io.ballerina.stdlib.kafka.utils.KafkaConstants.DURATION_UNDEFINED_VALUE;
import static io.ballerina.stdlib.kafka.utils.KafkaConstants.NATIVE_CONSUMER;
import static io.ballerina.stdlib.kafka.utils.KafkaConstants.NATIVE_CONSUMER_CONFIG;
import static io.ballerina.stdlib.kafka.utils.KafkaUtils.createKafkaError;
import static io.ballerina.stdlib.kafka.utils.KafkaUtils.getDefaultApiTimeout;
import static io.ballerina.stdlib.kafka.utils.KafkaUtils.getIntFromBDecimal;
import static io.ballerina.stdlib.kafka.utils.KafkaUtils.getPartitionToMetadataMap;

/**
 * Native methods to handle ballerina kafka consumer commits.
 */
public class Commit {

    private static final Logger logger = LoggerFactory.getLogger(Commit.class);

    /**
     * Commit messages for the consumer.
     *
     * @param consumerObject Kafka consumer object from ballerina.
     * @return {@code BError}, if there's any error, null otherwise.
     */
    public static Object commit(Environment environment, BObject consumerObject) {
        KafkaTracingUtil.traceResourceInvocation(environment, consumerObject);
        KafkaConsumer kafkaConsumer = (KafkaConsumer) consumerObject.getNativeData(NATIVE_CONSUMER);
        try {
            synchronized (kafkaConsumer) {
                kafkaConsumer.commitSync();
            }
        } catch (KafkaException e) {
            KafkaMetricsUtil.reportConsumerError(consumerObject, KafkaObservabilityConstants.ERROR_TYPE_COMMIT);
            return createKafkaError("Failed to commit offsets: " + e.getMessage());
        }
        return null;
    }

    /**
     * Commit given offsets for the ballerina kafka consumer.
     *
     * @param consumerObject Kafka consumer object from ballerina.
     * @param offsets        Array of Partition offsets to commit.
     * @param duration       Duration in milliseconds to try the operation.
     * @return {@code BError}, if there's any error, null otherwise.
     */
    public static Object commitOffset(Environment environment, BObject consumerObject, BArray offsets,
                                      BDecimal duration) {
        KafkaTracingUtil.traceResourceInvocation(environment, consumerObject);
        KafkaConsumer kafkaConsumer = (KafkaConsumer) consumerObject.getNativeData(NATIVE_CONSUMER);

        Properties consumerProperties = (Properties) consumerObject.getNativeData(NATIVE_CONSUMER_CONFIG);
        int defaultApiTimeout = getDefaultApiTimeout(consumerProperties);
        int apiTimeout = getIntFromBDecimal(duration, logger, ALIAS_DURATION);
        Map<TopicPartition, OffsetAndMetadata> partitionToMetadataMap = getPartitionToMetadataMap(offsets);
        try {
            synchronized (kafkaConsumer) {
                // API timeout should given the priority over the default value
                if (apiTimeout > DURATION_UNDEFINED_VALUE) {
                    consumerCommitSyncWithDuration(kafkaConsumer, partitionToMetadataMap, apiTimeout);
                } else if (defaultApiTimeout > DURATION_UNDEFINED_VALUE) {
                    consumerCommitSyncWithDuration(kafkaConsumer, partitionToMetadataMap, defaultApiTimeout);
                } else {
                    kafkaConsumer.commitSync(partitionToMetadataMap);
                }
            }
        } catch (KafkaException e) {
            KafkaMetricsUtil.reportConsumerError(consumerObject, KafkaObservabilityConstants.ERROR_TYPE_COMMIT);
            return createKafkaError("Failed to commit the offset: " + e.getMessage());
        }
        return null;
    }

    private static void consumerCommitSyncWithDuration(KafkaConsumer consumer,
                                                       Map<TopicPartition, OffsetAndMetadata> metadataMap,
                                                       long timeout) {

        Duration duration = Duration.ofMillis(timeout);
        consumer.commitSync(metadataMap, duration);
    }
}
