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

package io.ballerina.stdlib.kafka.observability;

import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.observability.ObserveUtils;
import io.ballerina.runtime.observability.metrics.DefaultMetricRegistry;
import io.ballerina.runtime.observability.metrics.MetricId;
import io.ballerina.runtime.observability.metrics.MetricRegistry;
import io.ballerina.stdlib.kafka.utils.KafkaUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Set;

/**
 * Providing metrics functionality to the Kafka connector.
 *
 * @since 1.2.0
 */
public class KafkaMetricsUtil {

    private static final MetricRegistry metricRegistry = DefaultMetricRegistry.getInstance();

    /**
     * Reports a new producer connection.
     *
     * @param producerObject producerObject.
     */
    public static void reportNewProducer(BObject producerObject) {
        if (!ObserveUtils.isMetricsEnabled()) {
            return;
        }
        KafkaObserverContext observerContext = new KafkaObserverContext(KafkaObservabilityConstants.CONTEXT_PRODUCER,
                                                                        KafkaUtils.getClientId(producerObject),
                                                                        KafkaUtils.getBootstrapServers(producerObject));
        incrementGauge(observerContext, KafkaObservabilityConstants.METRIC_PUBLISHERS[0],
                       KafkaObservabilityConstants.METRIC_PUBLISHERS[1]);
    }

    /**
     * Reports a new consumer connection.
     *
     * @param consumerObject consumer object
     */
    public static void reportNewConsumer(BObject consumerObject) {
        if (!ObserveUtils.isMetricsEnabled()) {
            return;
        }
        KafkaObserverContext observerContext = new KafkaObserverContext(KafkaObservabilityConstants.CONTEXT_CONSUMER,
                                                                        KafkaUtils.getClientId(consumerObject),
                                                                        KafkaUtils.getBootstrapServers(consumerObject));
        incrementGauge(observerContext, KafkaObservabilityConstants.METRIC_CONSUMERS[0],
                       KafkaObservabilityConstants.METRIC_CONSUMERS[1]);
    }

    /**
     * Reports a producer disconnection.
     *
     * @param producerObject producer object.
     */
    public static void reportProducerClose(BObject producerObject) {
        if (!ObserveUtils.isMetricsEnabled()) {
            return;
        }
        KafkaObserverContext observerContext = new KafkaObserverContext(KafkaObservabilityConstants.CONTEXT_PRODUCER,
                                                                        KafkaUtils.getClientId(producerObject),
                                                                        KafkaUtils.getBootstrapServers(producerObject));
        decrementGauge(observerContext, KafkaObservabilityConstants.METRIC_PUBLISHERS[0],
                       KafkaObservabilityConstants.METRIC_PUBLISHERS[1]);
    }

    /**
     * Reports a consumer close.
     *
     * @param consumerObject consumer object.
     */
    public static void reportConsumerClose(BObject consumerObject) {
        if (!ObserveUtils.isMetricsEnabled()) {
            return;
        }
        KafkaObserverContext observerContext = new KafkaObserverContext(KafkaObservabilityConstants.CONTEXT_CONSUMER,
                                                                        KafkaUtils.getClientId(consumerObject),
                                                                        KafkaUtils.getBootstrapServers(consumerObject));
        decrementGauge(observerContext, KafkaObservabilityConstants.METRIC_CONSUMERS[0],
                       KafkaObservabilityConstants.METRIC_CONSUMERS[1]);
    }

    /**
     * Reports a message being published by a Kafka producer.
     *
     * @param producerObject producer object.
     * @param topic          Subject the message is published to.
     * @param value          Message object.
     */
    public static void reportPublish(BObject producerObject, String topic, Object value) {
        if (!ObserveUtils.isMetricsEnabled()) {
            return;
        }
        KafkaObserverContext observerContext = new KafkaObserverContext(KafkaObservabilityConstants.CONTEXT_PRODUCER,
                                                                        KafkaUtils.getClientId(producerObject),
                                                                        KafkaUtils.getBootstrapServers(producerObject),
                                                                        topic);
        int size = getMessageSize(value);
        reportPublish(observerContext, size);
    }

    private static int getMessageSize(Object message) {
        int size = 0;
        if (message instanceof String) {
            byte[] bytes = ((String) message).getBytes(StandardCharsets.UTF_8);
            size = bytes.length;
        } else if (message instanceof Long || message instanceof Double) {
            size = Double.BYTES;
        } else if (message instanceof byte[]) {
            size = ((byte[]) message).length;
        } else {
            try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
                ObjectOutputStream oos = new ObjectOutputStream(bos);
                oos.writeObject(message);
                oos.flush();
                byte[] data = bos.toByteArray();
                size = data.length;
            } catch (IOException e) {
                LoggerFactory.getLogger(KafkaMetricsUtil.class).error(e.getMessage());
            }
        }
        return size;
    }

    /**
     * Reports a consumer subscribing to a subject.
     *
     * @param consumerObject Consumer object.
     * @param topicsList     List of topics that the consumer subscribes to.
     */
    public static void reportBulkSubscription(BObject consumerObject, Set<String> topicsList) {
        if (!ObserveUtils.isMetricsEnabled()) {
            return;
        }
        for (String topic : topicsList) {
            reportSubscription(consumerObject, topic);
        }
    }

    /**
     * Reports a consumer subscribing to a topic.
     *
     * @param consumerObject Consumer object.
     * @param topic          Topic that the consumer subscribes to.
     */
    public static void reportSubscription(BObject consumerObject, String topic) {
        if (!ObserveUtils.isMetricsEnabled()) {
            return;
        }
        KafkaObserverContext observerContext = new KafkaObserverContext(KafkaObservabilityConstants.CONTEXT_CONSUMER,
                                                                        KafkaUtils.getClientId(consumerObject),
                                                                        KafkaUtils.getBootstrapServers(consumerObject),
                                                                        topic);
        setGauge(observerContext, KafkaObservabilityConstants.METRIC_SUBSCRIPTION[0],
                 KafkaObservabilityConstants.METRIC_SUBSCRIPTION[1], 1);
    }

    /**
     * Reports a consumer unsubscribing from a subject.
     *
     * @param consumerObject Consumer object.
     * @param topic          Subject that the consumer unsubscribes from.
     */
    public static void reportUnsubscription(BObject consumerObject, String topic) {
        if (!ObserveUtils.isMetricsEnabled()) {
            return;
        }
        KafkaObserverContext observerContext = new KafkaObserverContext(KafkaObservabilityConstants.CONTEXT_CONSUMER,
                                                                        KafkaUtils.getClientId(consumerObject),
                                                                        KafkaUtils.getBootstrapServers(consumerObject),
                                                                        topic);
        resetGauge(observerContext, KafkaObservabilityConstants.METRIC_SUBSCRIPTION[0],
                   KafkaObservabilityConstants.METRIC_SUBSCRIPTION[1]);
    }

    /**
     * Reports a consumer unsubscribing from multiple topics.
     *
     * @param consumerObject Consumer object.
     * @param topics         Subjects that the consumer unsubscribes from.
     */
    public static void reportBulkUnsubscription(BObject consumerObject, Set<String> topics) {
        if (!ObserveUtils.isMetricsEnabled()) {
            return;
        }
        for (String subject : topics) {
            KafkaMetricsUtil.reportUnsubscription(consumerObject, subject);
        }
    }

    /**
     * Reports a message consumption of a consumer.
     *
     * @param consumerObject Consumer object.
     * @param topic          Subject that the consumer receives the message from.
     * @param value          Value of the received Kafka record.
     */
    public static void reportConsume(BObject consumerObject, String topic, Object value) {
        if (!ObserveUtils.isMetricsEnabled()) {
            return;
        }
        KafkaObserverContext observerContext = new KafkaObserverContext(KafkaObservabilityConstants.CONTEXT_CONSUMER,
                                                                        KafkaUtils.getClientId(consumerObject),
                                                                        KafkaUtils.getBootstrapServers(consumerObject),
                                                                        topic);
        int size = getMessageSize(value);
        reportConsume(observerContext, size);
    }

    /**
     * Reports a consumer consuming a record of messages.
     *
     * @param consumerObject Consumer object.
     * @param records        Records
     */
    public static void reportConsume(BObject consumerObject, ConsumerRecords records) {
        if (!ObserveUtils.isMetricsEnabled()) {
            return;
        }
        for (Object record : records) {
            KafkaMetricsUtil.reportConsume(consumerObject, ((ConsumerRecord) record).topic(),
                                           ((ConsumerRecord) record).serializedValueSize());
        }
    }

    /**
     * Reports an error generated by a producer. This method is called when the URL/subject of the current producer is
     * unknown. e.g. when a Kafka connection doesn't exist for a producer.
     *
     * @param producerObject Producer object.
     * @param errorType      Type of the error.
     */
    public static void reportProducerError(BObject producerObject, String errorType) {
        if (!ObserveUtils.isMetricsEnabled()) {
            return;
        }
        reportError(KafkaObservabilityConstants.CONTEXT_PRODUCER, producerObject, errorType);
    }

    /**
     * Reports an error generated by a consumer. This method is called when the URL/subject of the current consumer is
     * unknown. e.g. when a Kafka connection doesn't exist for a consumer.
     *
     * @param errorType type of the error.
     */
    public static void reportConsumerError(String errorType) {
        if (!ObserveUtils.isMetricsEnabled()) {
            return;
        }
        reportError(KafkaObservabilityConstants.CONTEXT_CONSUMER, errorType);
    }

    /**
     * Reports an error generated by a consumer. This method is called when the URL/subject of the current consumer is
     * unknown. e.g. when a Kafka connection doesn't exist for a consumer.
     *
     * @param consumerObject Consumer object.
     * @param errorType      Type of the error.
     */
    public static void reportConsumerError(BObject consumerObject, String errorType) {
        if (!ObserveUtils.isMetricsEnabled()) {
            return;
        }
        reportError(KafkaObservabilityConstants.CONTEXT_CONSUMER, consumerObject, errorType);
    }

    /**
     * Reports an error generated by a consumer.
     *
     * @param consumerObject Consumer object.
     * @param topic          Subject that the consumer is subscribed to.
     * @param errorType      Type of the error.
     */
    public static void reportConsumerError(BObject consumerObject, String topic, String errorType) {
        if (!ObserveUtils.isMetricsEnabled()) {
            return;
        }
        reportError(consumerObject, topic, KafkaObservabilityConstants.CONTEXT_CONSUMER, errorType);
    }

    private static void reportPublish(KafkaObserverContext observerContext, int size) {
        incrementCounter(observerContext, KafkaObservabilityConstants.METRIC_PUBLISHED[0],
                         KafkaObservabilityConstants.METRIC_PUBLISHED[1]);
        incrementCounter(observerContext, KafkaObservabilityConstants.METRIC_PUBLISHED_SIZE[0],
                         KafkaObservabilityConstants.METRIC_PUBLISHED_SIZE[1], size);
    }

    private static void reportConsume(KafkaObserverContext observerContext, int size) {
        incrementCounter(observerContext, KafkaObservabilityConstants.METRIC_CONSUMED[0],
                         KafkaObservabilityConstants.METRIC_CONSUMED[1]);
        incrementCounter(observerContext, KafkaObservabilityConstants.METRIC_CONSUMED_SIZE[0],
                         KafkaObservabilityConstants.METRIC_CONSUMED_SIZE[1], size);
    }

    public static void reportError(String context, String errorType) {
        KafkaObserverContext observerContext = new KafkaObserverContext(context);
        observerContext.addTag(KafkaObservabilityConstants.TAG_ERROR_TYPE, errorType);
        incrementCounter(observerContext, KafkaObservabilityConstants.METRIC_ERRORS[0],
                         KafkaObservabilityConstants.METRIC_ERRORS[1]);
    }

    public static void reportError(String context, BObject object, String errorType) {
        KafkaObserverContext observerContext = new KafkaObserverContext(context,
                                                                        KafkaUtils.getClientId(object),
                                                                        KafkaUtils.getBootstrapServers(object));
        observerContext.addTag(KafkaObservabilityConstants.TAG_ERROR_TYPE, errorType);
        incrementCounter(observerContext, KafkaObservabilityConstants.METRIC_ERRORS[0],
                         KafkaObservabilityConstants.METRIC_ERRORS[1]);
    }

    public static void reportError(BObject object, String topic, String context, String errorType) {
        KafkaObserverContext observerContext = new KafkaObserverContext(context,
                                                                        KafkaUtils.getClientId(object),
                                                                        KafkaUtils.getBootstrapServers(object),
                                                                        topic);
        observerContext.addTag(KafkaObservabilityConstants.TAG_ERROR_TYPE, errorType);
        incrementCounter(observerContext, KafkaObservabilityConstants.METRIC_ERRORS[0],
                         KafkaObservabilityConstants.METRIC_ERRORS[1]);
    }

    private static void incrementCounter(KafkaObserverContext observerContext, String name, String desc) {
        incrementCounter(observerContext, name, desc, 1);
    }

    private static void incrementCounter(KafkaObserverContext observerContext, String name, String desc, int amount) {
        if (metricRegistry == null) {
            return;
        }
        metricRegistry.counter(new MetricId(
                KafkaObservabilityConstants.CONNECTOR_NAME + "_" + name, desc, observerContext.getAllTags()))
                .increment(amount);
    }

    private static void incrementGauge(KafkaObserverContext observerContext, String name, String desc) {
        if (metricRegistry == null) {
            return;
        }
        metricRegistry.gauge(new MetricId(
                KafkaObservabilityConstants.CONNECTOR_NAME + "_" + name, desc, observerContext.getAllTags()))
                .increment();
    }

    private static void decrementGauge(KafkaObserverContext observerContext, String name, String desc) {
        if (metricRegistry == null) {
            return;
        }
        metricRegistry.gauge(new MetricId(
                KafkaObservabilityConstants.CONNECTOR_NAME + "_" + name, desc, observerContext.getAllTags()))
                .decrement();
    }

    private static void setGauge(KafkaObserverContext observerContext, String name, String desc, int value) {
        if (metricRegistry == null) {
            return;
        }
        metricRegistry.gauge(new MetricId(
                KafkaObservabilityConstants.CONNECTOR_NAME + "_" + name, desc, observerContext.getAllTags()))
                .setValue(value);
    }

    private static void resetGauge(KafkaObserverContext observerContext, String name, String desc) {
        if (metricRegistry == null) {
            return;
        }
        metricRegistry.gauge(new MetricId(
                KafkaObservabilityConstants.CONNECTOR_NAME + "_" + name, desc, observerContext.getAllTags()))
                .setValue(0);
    }

    private KafkaMetricsUtil() {
    }
}
