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
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.api.values.BString;
import io.ballerina.stdlib.kafka.observability.KafkaMetricsUtil;
import io.ballerina.stdlib.kafka.observability.KafkaObservabilityConstants;
import io.ballerina.stdlib.kafka.observability.KafkaTracingUtil;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

import static io.ballerina.stdlib.kafka.utils.KafkaConstants.NATIVE_CONSUMER;
import static io.ballerina.stdlib.kafka.utils.KafkaConstants.SUBSCRIBED_TOPICS;
import static io.ballerina.stdlib.kafka.utils.KafkaUtils.createKafkaError;
import static io.ballerina.stdlib.kafka.utils.KafkaUtils.getStringListFromStringBArray;
import static io.ballerina.stdlib.kafka.utils.KafkaUtils.getTopicNamesString;

/**
 * Native methods to handle subscription of the ballerina kafka consumer.
 */
public class SubscriptionHandler {

    private static final Logger logger = LoggerFactory.getLogger(SubscriptionHandler.class);

    /**
     * Subscribe the ballerina kafka consumer to the given array of topics.
     *
     * @param consumerObject Kafka consumer object from ballerina.
     * @param topics         Ballerina {@code string[]} of topics.
     * @return {@code BError}, if there's any error, null otherwise.
     */
    public static Object subscribe(Environment environment, BObject consumerObject, BArray topics) {
        KafkaTracingUtil.traceResourceInvocation(environment, consumerObject);
        KafkaConsumer kafkaConsumer = (KafkaConsumer) consumerObject.getNativeData(NATIVE_CONSUMER);
        List<String> topicsList = getStringListFromStringBArray(topics);
        consumerObject.addNativeData("topics", topicsList);
        try {
            kafkaConsumer.subscribe(topicsList);
            Set<String> subscribedTopics = kafkaConsumer.subscription();
            KafkaMetricsUtil.reportBulkSubscription(consumerObject, subscribedTopics);
        } catch (IllegalArgumentException | IllegalStateException | KafkaException e) {
            KafkaMetricsUtil.reportConsumerError(consumerObject, KafkaObservabilityConstants.ERROR_TYPE_SUBSCRIBE);
            return createKafkaError("Failed to subscribe to the provided topics: " + e.getMessage());
        }
        if (logger.isDebugEnabled()) {
            logger.debug(SUBSCRIBED_TOPICS + getTopicNamesString(topicsList));
        }
        return null;
    }

    /**
     * Subscribes the ballerina kafka consumer to the topics matching the given regex.
     *
     * @param consumerObject Kafka consumer object from ballerina.
     * @param topicRegex     Regex to match topics to subscribe.
     * @return {@code BError}, if there's any error, null otherwise.
     */
    public static Object subscribeToPattern(Environment environment, BObject consumerObject, BString topicRegex) {
        KafkaTracingUtil.traceResourceInvocation(environment, consumerObject);
        KafkaConsumer kafkaConsumer = (KafkaConsumer) consumerObject.getNativeData(NATIVE_CONSUMER);
        try {
            kafkaConsumer.subscribe(Pattern.compile(topicRegex.getValue()));
            // TODO: This sometimes not updating since Kafka not updates the subscription tight away
            Set<String> topicsList = kafkaConsumer.subscription();
            KafkaMetricsUtil.reportBulkSubscription(consumerObject, topicsList);
        } catch (IllegalArgumentException | IllegalStateException | KafkaException e) {
            KafkaMetricsUtil.reportConsumerError(consumerObject,
                                                 KafkaObservabilityConstants.ERROR_TYPE_SUBSCRIBE_PATTERN);
            return createKafkaError("Failed to subscribe to the topics: " + e.getMessage());
        }
        return null;
    }

    /**
     * Unsubscribe the ballerina kafka consumer from all the topics.
     *
     * @param consumerObject Kafka consumer object from ballerina.
     * @return {@code BError}, if there's any error, null otherwise.
     */
    public static Object unsubscribe(Environment environment, BObject consumerObject) {
        KafkaTracingUtil.traceResourceInvocation(environment, consumerObject);
        KafkaConsumer kafkaConsumer = (KafkaConsumer) consumerObject.getNativeData(NATIVE_CONSUMER);
        try {
            Set<String> topics = kafkaConsumer.subscription();
            kafkaConsumer.unsubscribe();
            KafkaMetricsUtil.reportBulkUnsubscription(consumerObject, topics);
        } catch (KafkaException e) {
            KafkaMetricsUtil.reportConsumerError(consumerObject, KafkaObservabilityConstants.ERROR_TYPE_UNSUBSCRIBE);
            return createKafkaError("Failed to unsubscribe the consumer: " + e.getMessage());
        }
        return null;
    }
}
