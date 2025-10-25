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

import io.ballerina.runtime.api.values.BError;
import io.ballerina.stdlib.kafka.api.KafkaListener;
import io.ballerina.stdlib.kafka.utils.KafkaConstants;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * {@code KafkaRecordConsumer} This class represents Runnable flow which periodically poll the remote broker and fetch
 * Kafka records.
 */
public class KafkaRecordConsumer {

    private final AtomicBoolean closed = new AtomicBoolean(false);

    private static final Logger logger = LoggerFactory.getLogger(KafkaRecordConsumer.class);

    private KafkaConsumer kafkaConsumer;
    private Duration pollingTimeout = Duration.ofMillis(1000);
    private int pollingInterval = 1000;
    private long stopTimeout = 30000;
    private String groupId;
    private final KafkaListener kafkaListener;
    private final String serviceId;
    private final int consumerId;
    private final ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);
    private ScheduledFuture pollTaskFuture;

    public KafkaRecordConsumer(KafkaListener kafkaListener, Properties configParams, String serviceId, int consumerId,
                               KafkaConsumer kafkaConsumer) {
        this.serviceId = serviceId;
        this.consumerId = consumerId;
        // Initialize Kafka Consumer.
        if (Objects.isNull(kafkaConsumer)) {
            this.kafkaConsumer = new KafkaConsumer<>(configParams);
        } else {
            this.kafkaConsumer = kafkaConsumer;
        }
        List<String> topics = (List<String>) configParams.get(KafkaConstants.ALIAS_TOPICS.getValue());
        // Subscribe Kafka Consumer to given topics.
        // Note: This is where the connection to brokers is actually established
        // SSL/TLS handshake and SASL authentication will occur here
        this.kafkaConsumer.subscribe(topics);
        this.kafkaListener = kafkaListener;
        if (configParams.get(KafkaConstants.ALIAS_POLLING_TIMEOUT.getValue()) != null) {
            this.pollingTimeout = Duration.ofMillis((Integer)
                    configParams.get(KafkaConstants.ALIAS_POLLING_TIMEOUT.getValue()));
        }
        if (configParams.get(KafkaConstants.ALIAS_POLLING_INTERVAL.getValue()) != null) {
            this.pollingInterval = (Integer) configParams.get(KafkaConstants.ALIAS_POLLING_INTERVAL.getValue());
        }
        this.groupId = (String) configParams.get(ConsumerConfig.GROUP_ID_CONFIG);
    }

    private void poll() {
        try {
            ConsumerRecords recordsRetrieved = null;
            try {
                // Make thread-safe as kafka does not support multiple thread access
                if (!closed.get()) {
                    recordsRetrieved = this.kafkaConsumer.poll(this.pollingTimeout);
                }
            } catch (WakeupException e) {
                // Ignore exception if connection is closing.
                if (!closed.get()) {
                    throw e;
                }
            }
            if (logger.isDebugEnabled()) {
                logger.debug("Kafka service " + this.serviceId + " attached to consumer "
                                     + this.consumerId + " has received " + recordsRetrieved.count() + " records.");
            }
            processRetrievedRecords(recordsRetrieved);
        } catch (KafkaException | IllegalStateException | IllegalArgumentException e) {
            // Log detailed error information for diagnostics
            String detailedError = getDetailedErrorMessage(e);
            logger.error("Kafka service {} consumer {} encountered error while polling: {}",
                        this.serviceId, this.consumerId, detailedError, e);
            this.kafkaListener.onError(new KafkaException("Failed to poll from Kafka: " + detailedError, e));
            // When un-recoverable exception is thrown we stop scheduling task to the executor.
            // Later at stopConsume() on KafkaRecordConsumer we close the consumer.
            this.pollTaskFuture.cancel(false);
        } catch (BError e) {
            this.kafkaListener.onError(e);
        }
    }

    private void processRetrievedRecords(ConsumerRecords consumerRecords) {
        if (Objects.nonNull(consumerRecords) && !consumerRecords.isEmpty()) {
            Semaphore sem = new Semaphore(0);
            KafkaPollCycleFutureListener pollCycleListener = new KafkaPollCycleFutureListener(sem, serviceId);
            this.kafkaListener.onRecordsReceived(consumerRecords, kafkaConsumer, groupId, pollCycleListener);
            // We suspend execution of poll cycle here before moving to the next cycle.
            // Once we receive signal from BVM via KafkaPollCycleFutureListener this suspension is removed
            // We will move to the next polling cycle.
            try {
                sem.acquire();
            } catch (InterruptedException e) {
                this.kafkaListener.onError(e);
                this.pollTaskFuture.cancel(false);
            }
        }
    }

    /**
     * Starts Kafka consumer polling cycles, schedules thread pool for given polling cycle.
     */
    public void consume() {
        final Runnable pollingFunction = () -> poll();
        this.pollTaskFuture = this.executorService.scheduleAtFixedRate(pollingFunction, 0, this.pollingInterval,
                                                                       TimeUnit.MILLISECONDS);
    }

    /**
     * Returns current consumer id.
     *
     * @return consumer id integer.
     */
    public int getConsumerId() {
        return this.consumerId;
    }

    /**
     * Stops Kafka consumer polling cycles, schedules consumer close and shutdowns scheduled thread pool.
     */
    public void gracefulStopConsume() {
        // Make closed true, therefore poll function stops polling, and make stop operation thread-safe
        closed.set(true);
        this.kafkaConsumer.wakeup();
        final Runnable stopFunction = () -> this.kafkaConsumer.close();
        this.executorService.schedule(stopFunction, 0, TimeUnit.MILLISECONDS);
        this.executorService.shutdown();
    }

    /**
     * Stops Kafka consumer polling cycles, forcefully shutdowns scheduled thread pool and closes the consumer instance.
     */
    public void immediateStopConsume() {
        // Make closed true, therefore poll function stops polling, and make stop operation thread-safe
        closed.set(true);
        this.kafkaConsumer.wakeup();
        this.pollTaskFuture.cancel(true);
        this.executorService.shutdownNow();
        try {
            this.executorService.awaitTermination(stopTimeout, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            // (Re-)Cancel if current thread also interrupted
            this.executorService.shutdownNow();
            // Preserve interrupt status
            Thread.currentThread().interrupt();
        }
        this.kafkaConsumer.close(Duration.ofMillis(0));
    }

    /**
     * Stops Kafka consumer polling cycles and forcefully shutdowns scheduled thread pool.
     */
    public void stopScheduledPollTask() {
        closed.set(true);
        this.kafkaConsumer.wakeup();
        this.pollTaskFuture.cancel(true);
        this.executorService.shutdownNow();
        try {
            this.executorService.awaitTermination(stopTimeout, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            this.executorService.shutdownNow();
            Thread.currentThread().interrupt();
        }
        this.kafkaConsumer.unsubscribe();
    }

    /**
     * Extracts detailed error message from exceptions during poll operations.
     * Provides specific guidance for SSL, authentication, and connection errors.
     *
     * @param e The caught exception.
     * @return Detailed error message with cause information and troubleshooting guidance.
     */
    private static String getDetailedErrorMessage(Exception e) {
        Throwable cause = e.getCause() != null ? e.getCause() : e;
        String message = cause.getMessage() != null ? cause.getMessage() : e.getMessage();
        String causeClass = cause.getClass().getSimpleName();

        // Provide specific guidance for common error types
        if (message.contains("SSL") || message.contains("ssl") || causeClass.contains("SSL")) {
            return message + ". SSL/TLS error occurred. Please verify: " +
                   "1) Certificate paths are correct, " +
                   "2) Truststore/keystore are accessible and valid, " +
                   "3) Certificates are not expired, " +
                   "4) SSL protocol versions match broker configuration.";
        } else if (message.contains("SaslAuthentication") || message.contains("Authentication failed") ||
                   message.contains("SASL") || message.contains("authentication") || causeClass.contains("Sasl")) {
            return message + ". SASL authentication error occurred. Please verify: " +
                   "1) Username and password are correct, " +
                   "2) Authentication mechanism (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512) matches broker configuration, " +
                   "3) User has necessary permissions on the broker.";
        } else if (message.contains("TimeoutException") || message.contains("timeout") ||
                   causeClass.contains("Timeout")) {
            return message + ". Connection timeout occurred. Please verify: " +
                   "1) Bootstrap servers configuration is correct, " +
                   "2) Kafka brokers are running and accessible, " +
                   "3) Network connectivity and firewall rules allow connection, " +
                   "4) Consider increasing timeout values if network latency is high.";
        } else if (message.contains("UnknownHostException") || message.contains("nodename nor servname provided") ||
                   causeClass.contains("UnknownHost")) {
            return message + ". Cannot resolve broker hostname. Please verify: " +
                   "1) Bootstrap servers hostnames are spelled correctly, " +
                   "2) DNS resolution is working properly, " +
                   "3) Hostnames are reachable from this network.";
        } else if (message.contains("Connection refused") || message.contains("Connection reset") ||
                   message.contains("ConnectionException") || causeClass.contains("Connection")) {
            return message + ". Connection to broker failed. Please verify: " +
                   "1) Kafka brokers are running, " +
                   "2) Port numbers are correct in bootstrap servers, " +
                   "3) Network route to brokers is available, " +
                   "4) Firewall is not blocking the connection.";
        } else if (message.contains("NotLeaderForPartitionException") || message.contains("LeaderNotAvailable")) {
            return message + ". Kafka broker leadership issue. This may be temporary during broker restart or " +
                   "leader election. Retry the operation or verify cluster health.";
        } else {
            return message;
        }
    }
}
