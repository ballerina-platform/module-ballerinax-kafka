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

package io.ballerina.stdlib.kafka.impl;

import io.ballerina.stdlib.kafka.api.KafkaListener;
import io.ballerina.stdlib.kafka.api.KafkaServerConnector;
import io.ballerina.stdlib.kafka.exceptions.KafkaConnectorException;
import io.ballerina.stdlib.kafka.utils.KafkaConstants;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaException;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * {@code KafkaServerConnectorImpl} This is the implementation for the {@code KafkaServerConnector} API which provides
 * transport receiver implementation for Kafka.
 */
public class KafkaServerConnectorImpl implements KafkaServerConnector {

    private String serviceId;
    private KafkaListener kafkaListener;
    private Properties configParams;
    private int numOfConcurrentConsumers = 1;
    private List<KafkaRecordConsumer> messageConsumers;
    private KafkaConsumer kafkaConsumer;

    public KafkaServerConnectorImpl(String serviceId, Properties configParams, KafkaListener kafkaListener,
                                    KafkaConsumer kafkaConsumer) throws KafkaConnectorException {
        this.kafkaListener = kafkaListener;
        this.serviceId = serviceId;
        if (configParams.get(KafkaConstants.ALIAS_CONCURRENT_CONSUMERS.getValue()) != null) {
            this.numOfConcurrentConsumers =
                    (Integer) configParams.get(KafkaConstants.ALIAS_CONCURRENT_CONSUMERS.getValue());
        }
        if (this.numOfConcurrentConsumers <= 0) {
            throw new KafkaConnectorException(
                    "Number of Concurrent consumers should be a positive integer value greater than zero.");
        }
        this.configParams = configParams;
        this.kafkaConsumer = kafkaConsumer;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void start() throws KafkaConnectorException {
        try {
            this.messageConsumers = new ArrayList<>();
            for (int counter = 0; counter < numOfConcurrentConsumers; counter++) {
                KafkaRecordConsumer consumer = new KafkaRecordConsumer(this.kafkaListener, this.configParams,
                                                                       this.serviceId, counter, this.kafkaConsumer);
                this.messageConsumers.add(consumer);
                consumer.consume();
            }
        } catch (KafkaException e) {
            throw new KafkaConnectorException(
                    "Error creating Kafka consumer to connect with remote broker and subscribe to provided topics", e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean gracefulStop() throws KafkaConnectorException {
        KafkaConnectorException ex = null;
        for (KafkaRecordConsumer consumer : this.messageConsumers) {
            try {
                consumer.gracefulStopConsume();
            } catch (KafkaException e) {
                if (ex == null) {
                    ex = new KafkaConnectorException("Error closing the Kafka consumers for service " + serviceId, e);
                } else {
                    ex.addSuppressed(e);
                }
            }
        }
        this.messageConsumers = null;
        if (ex != null) {
            throw ex;
        }
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean immediateStop() throws KafkaConnectorException {
        KafkaConnectorException ex = null;
        for (KafkaRecordConsumer consumer : this.messageConsumers) {
            try {
                consumer.immediateStopConsume();
            } catch (KafkaException e) {
                if (ex == null) {
                    ex = new KafkaConnectorException("Error closing the Kafka consumers for service " + serviceId, e);
                } else {
                    ex.addSuppressed(e);
                }
            }
        }
        this.messageConsumers = null;
        if (ex != null) {
            throw ex;
        }
        return true;
    }

    /**
     * Stops the scheduled poll task.
     *
     * @return true if stopped successfully, false otherwise
     * @throws KafkaConnectorException if error occurred while stopping the poll task
     */
    public boolean stopPollingTask() throws KafkaConnectorException {
        KafkaConnectorException kcException = null;
        for (KafkaRecordConsumer consumer : this.messageConsumers) {
            try {
                consumer.stopScheduledPollTask();
            } catch (KafkaException e) {
                if (kcException == null) {
                    kcException = new KafkaConnectorException("Error closing the Kafka consumers for service " +
                            serviceId, e);
                } else {
                    kcException.addSuppressed(e);
                }
            }
        }
        this.messageConsumers = null;
        if (kcException != null) {
            throw kcException;
        }
        return true;
    }
}
