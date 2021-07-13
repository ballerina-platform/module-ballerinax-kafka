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
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.api.values.BString;
import io.ballerina.stdlib.kafka.observability.KafkaMetricsUtil;
import io.ballerina.stdlib.kafka.observability.KafkaObservabilityConstants;
import io.ballerina.stdlib.kafka.observability.KafkaTracingUtil;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;

import static io.ballerina.stdlib.kafka.utils.KafkaConstants.ALIAS_OFFSET;
import static io.ballerina.stdlib.kafka.utils.KafkaConstants.NATIVE_CONSUMER;
import static io.ballerina.stdlib.kafka.utils.KafkaUtils.createKafkaError;
import static io.ballerina.stdlib.kafka.utils.KafkaUtils.createTopicPartitionFromPartitionOffset;
import static io.ballerina.stdlib.kafka.utils.KafkaUtils.getTopicPartitionList;

/**
 * Native methods to handle ballerina kafka consumer seek operations.
 */
public class Seek {

    private static final Logger logger = LoggerFactory.getLogger(Seek.class);

    /**
     * Seek ballerina kafka consumer to a given partition offset.
     *
     * @param consumerObject  Kafka consumer object from ballerina.
     * @param partitionOffset Partition offset record to seek.
     * @return {@code BError}, if there's any error, null otherwise.
     */
    public static Object seek(Environment environment, BObject consumerObject, BMap<BString, Object> partitionOffset) {
        KafkaTracingUtil.traceResourceInvocation(environment, consumerObject);
        KafkaConsumer kafkaConsumer = (KafkaConsumer) consumerObject.getNativeData(NATIVE_CONSUMER);
        TopicPartition topicPartition = createTopicPartitionFromPartitionOffset(partitionOffset);
        Long offset = partitionOffset.getIntValue(ALIAS_OFFSET);

        try {
            kafkaConsumer.seek(topicPartition, offset);
        } catch (IllegalStateException | IllegalArgumentException | KafkaException e) {
            KafkaMetricsUtil.reportConsumerError(consumerObject, KafkaObservabilityConstants.ERROR_TYPE_SEEK);
            return createKafkaError("Failed to seek the consumer: " + e.getMessage());
        }
        return null;
    }

    /**
     * Seek ballerina kafka consumer to the beginning.
     *
     * @param consumerObject  Kafka consumer object from ballerina.
     * @param topicPartitions Topic partitions to seek to the beginning.
     * @return {@code BError}, if there's any error, null otherwise.
     */
    public static Object seekToBeginning(Environment environment, BObject consumerObject, BArray topicPartitions) {
        KafkaTracingUtil.traceResourceInvocation(environment, consumerObject);
        KafkaConsumer kafkaConsumer = (KafkaConsumer) consumerObject.getNativeData(NATIVE_CONSUMER);
        ArrayList<TopicPartition> partitionList = getTopicPartitionList(topicPartitions, logger);
        try {
            kafkaConsumer.seekToBeginning(partitionList);
        } catch (IllegalStateException | IllegalArgumentException | KafkaException e) {
            KafkaMetricsUtil.reportConsumerError(consumerObject, KafkaObservabilityConstants.ERROR_TYPE_SEEK_BEG);
            return createKafkaError("Failed to seek the consumer to the beginning: " + e.getMessage());
        }
        return null;
    }

    /**
     * Seek ballerina kafka consumer to the end.
     *
     * @param consumerObject  Kafka consumer object from ballerina.
     * @param topicPartitions Topic partitions to seek to the end.
     * @return {@code BError}, if there's any error, null otherwise.
     */
    public static Object seekToEnd(Environment environment, BObject consumerObject, BArray topicPartitions) {
        KafkaTracingUtil.traceResourceInvocation(environment, consumerObject);
        KafkaConsumer kafkaConsumer = (KafkaConsumer) consumerObject.getNativeData(NATIVE_CONSUMER);
        ArrayList<TopicPartition> partitionList = getTopicPartitionList(topicPartitions, logger);
        try {
            kafkaConsumer.seekToEnd(partitionList);
        } catch (IllegalStateException | IllegalArgumentException | KafkaException e) {
            KafkaMetricsUtil.reportConsumerError(consumerObject, KafkaObservabilityConstants.ERROR_TYPE_SEEK_END);
            return createKafkaError("Failed to seek the consumer to the end: " + e.getMessage());
        }
        return null;
    }
}
