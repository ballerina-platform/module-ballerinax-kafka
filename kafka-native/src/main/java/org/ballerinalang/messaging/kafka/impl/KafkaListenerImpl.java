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

package org.ballerinalang.messaging.kafka.impl;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.ballerinalang.jvm.api.BRuntime;
import org.ballerinalang.jvm.api.connector.CallableUnitCallback;
import org.ballerinalang.jvm.api.values.BObject;
import org.ballerinalang.jvm.observability.ObservabilityConstants;
import org.ballerinalang.jvm.observability.ObserveUtils;
import org.ballerinalang.jvm.scheduling.Strand;
import org.ballerinalang.messaging.kafka.api.KafkaListener;
import org.ballerinalang.messaging.kafka.observability.KafkaMetricsUtil;
import org.ballerinalang.messaging.kafka.observability.KafkaObservabilityConstants;
import org.ballerinalang.messaging.kafka.observability.KafkaObserverContext;
import org.ballerinalang.messaging.kafka.utils.KafkaUtils;

import java.util.HashMap;
import java.util.Map;

import static org.ballerinalang.messaging.kafka.utils.KafkaConstants.KAFKA_RESOURCE_ON_MESSAGE;
import static org.ballerinalang.messaging.kafka.utils.KafkaConstants.NATIVE_CONSUMER;
import static org.ballerinalang.messaging.kafka.utils.KafkaConstants.ON_MESSAGE_METADATA;
import static org.ballerinalang.messaging.kafka.utils.KafkaUtils.getResourceParameters;

/**
 * Kafka Connector Consumer for Ballerina.
 */
public class KafkaListenerImpl implements KafkaListener {

    private BObject service;
    private BObject listener;
    private ResponseCallback callback;
    private BRuntime bRuntime;

    public KafkaListenerImpl(BObject listener, BObject service, BRuntime bRuntime) {
        this.bRuntime = bRuntime;
        this.listener = listener;
        this.service = service;
        callback = new ResponseCallback();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onRecordsReceived(ConsumerRecords records, KafkaConsumer kafkaConsumer, String groupId) {
        listener.addNativeData(NATIVE_CONSUMER, kafkaConsumer);
        executeResource(listener, records, groupId);
        KafkaMetricsUtil.reportConsume(listener, records);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onRecordsReceived(ConsumerRecords records, KafkaConsumer kafkaConsumer, String groupId,
                                  KafkaPollCycleFutureListener consumer) {
        listener.addNativeData(NATIVE_CONSUMER, kafkaConsumer);
        executeResource(listener, consumer, records, groupId);
        KafkaMetricsUtil.reportConsume(listener, records);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onError(Throwable throwable) {
        KafkaMetricsUtil.reportConsumerError(listener, KafkaObservabilityConstants.ERROR_TYPE_MSG_RECEIVED);
    }

    private void executeResource(BObject listener, ConsumerRecords records, String groupId) {
        if (ObserveUtils.isTracingEnabled()) {
            Map<String, Object> properties = getNewObserverContextInProperties(listener);
            bRuntime.invokeMethodAsync(service, KAFKA_RESOURCE_ON_MESSAGE, null, ON_MESSAGE_METADATA, callback,
                                      properties, getResourceParameters(service, this.listener, records, groupId));
        } else {
            bRuntime.invokeMethodAsync(service, KAFKA_RESOURCE_ON_MESSAGE, null, ON_MESSAGE_METADATA, callback,
                                      null, getResourceParameters(service, this.listener, records, groupId));
        }
    }

    private void executeResource(BObject listener, KafkaPollCycleFutureListener consumer, ConsumerRecords records,
                                 String groupId) {
        if (ObserveUtils.isTracingEnabled()) {
            Map<String, Object> properties = getNewObserverContextInProperties(listener);
            bRuntime.invokeMethodAsync(service, KAFKA_RESOURCE_ON_MESSAGE, null, ON_MESSAGE_METADATA, consumer,
                                      properties, getResourceParameters(service, this.listener, records, groupId));
        } else {
            bRuntime.invokeMethodAsync(service, KAFKA_RESOURCE_ON_MESSAGE, null, ON_MESSAGE_METADATA, consumer,
                                      null, getResourceParameters(service, this.listener, records, groupId));
        }
    }

    private Map<String, Object> getNewObserverContextInProperties(BObject listener) {
        Map<String, Object> properties = new HashMap<>();
        KafkaObserverContext observerContext = new KafkaObserverContext(KafkaObservabilityConstants.CONTEXT_CONSUMER,
                                                                        KafkaUtils.getClientId(listener),
                                                                        KafkaUtils.getBootstrapServers(listener));
        properties.put(ObservabilityConstants.KEY_OBSERVER_CONTEXT, observerContext);
        return properties;
    }

    private static class ResponseCallback implements CallableUnitCallback {

        @Override
        public void notifySuccess() {
            // do nothing
        }

        @Override
        public void notifyFailure(org.ballerinalang.jvm.api.values.BError error) {
            // do nothing
        }
    }
}
