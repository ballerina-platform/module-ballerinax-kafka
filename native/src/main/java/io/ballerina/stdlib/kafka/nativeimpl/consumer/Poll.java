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
import io.ballerina.runtime.api.creators.ValueCreator;
import io.ballerina.runtime.api.types.ArrayType;
import io.ballerina.runtime.api.types.IntersectionType;
import io.ballerina.runtime.api.types.RecordType;
import io.ballerina.runtime.api.utils.TypeUtils;
import io.ballerina.runtime.api.values.BArray;
import io.ballerina.runtime.api.values.BDecimal;
import io.ballerina.runtime.api.values.BError;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.api.values.BTypedesc;
import io.ballerina.stdlib.kafka.observability.KafkaMetricsUtil;
import io.ballerina.stdlib.kafka.observability.KafkaObservabilityConstants;
import io.ballerina.stdlib.kafka.observability.KafkaTracingUtil;
import io.ballerina.stdlib.kafka.utils.ModuleUtils;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import static io.ballerina.runtime.api.utils.TypeUtils.getReferredType;
import static io.ballerina.stdlib.kafka.utils.KafkaConstants.CONSTRAINT_VALIDATION;
import static io.ballerina.stdlib.kafka.utils.KafkaConstants.CONSUMER_CONFIG_FIELD_NAME;
import static io.ballerina.stdlib.kafka.utils.KafkaConstants.NATIVE_CONSUMER;
import static io.ballerina.stdlib.kafka.utils.KafkaUtils.createKafkaError;
import static io.ballerina.stdlib.kafka.utils.KafkaUtils.getAutoCommitConfig;
import static io.ballerina.stdlib.kafka.utils.KafkaUtils.getAutoSeekOnErrorConfig;
import static io.ballerina.stdlib.kafka.utils.KafkaUtils.getConsumerRecords;
import static io.ballerina.stdlib.kafka.utils.KafkaUtils.getMilliSeconds;
import static io.ballerina.stdlib.kafka.utils.KafkaUtils.getValuesWithIntendedType;

/**
 * Native function polls the broker to retrieve messages within given timeout.
 */
public class Poll {

    private static final Logger logger = LoggerFactory.getLogger(Poll.class);

    // static init
    private static final ExecutorService executorService = Executors.newCachedThreadPool(new KafkaThreadFactory());

    public static Object poll(Environment env, BObject consumerObject, BDecimal timeout, BTypedesc bTypedesc) {
        KafkaTracingUtil.traceResourceInvocation(env, consumerObject);
        CompletableFuture<Object> balFuture = new CompletableFuture<>();
        KafkaConsumer kafkaConsumer = (KafkaConsumer) consumerObject.getNativeData(NATIVE_CONSUMER);
        RecordType recordType = getRecordType(bTypedesc);
        Thread.startVirtualThread(() -> {
            try {
                Duration duration = Duration.ofMillis(getMilliSeconds(timeout));
                boolean constraintValidation = (boolean) consumerObject.getMapValue(CONSUMER_CONFIG_FIELD_NAME)
                        .get(CONSTRAINT_VALIDATION);
                boolean autoCommit = getAutoCommitConfig(consumerObject);
                boolean autoSeek = getAutoSeekOnErrorConfig(consumerObject);
                BArray consumerRecords;
                synchronized (kafkaConsumer) {
                    ConsumerRecords recordsRetrieved = kafkaConsumer.poll(duration);
                    consumerRecords = getConsumerRecords(consumerObject, recordsRetrieved, recordType,
                            bTypedesc.getDescribingType().isReadOnly(), constraintValidation, autoCommit,
                            kafkaConsumer, autoSeek);
                }
                balFuture.complete(consumerRecords);
            } catch (IllegalStateException | IllegalArgumentException | KafkaException e) {
                KafkaMetricsUtil.reportConsumerError(consumerObject, KafkaObservabilityConstants.ERROR_TYPE_POLL);
                String detailedError = getDetailedErrorMessage(e);
                logger.error("Failed to poll from Kafka server: {}", detailedError, e);
                balFuture.complete(createKafkaError("Failed to poll from the Kafka server: " + detailedError));
            } catch (BError e) {
                KafkaMetricsUtil.reportConsumerError(consumerObject, KafkaObservabilityConstants.ERROR_TYPE_POLL);
                balFuture.complete(e);
            }
        });
        return ModuleUtils.getResult(balFuture);
    }

    public static Object pollPayload(Environment env, BObject consumerObject, BDecimal timeout, BTypedesc bTypedesc) {
        KafkaTracingUtil.traceResourceInvocation(env, consumerObject);
        CompletableFuture<Object> balFuture = new CompletableFuture<>();
        KafkaConsumer kafkaConsumer = (KafkaConsumer) consumerObject.getNativeData(NATIVE_CONSUMER);
        Thread.startVirtualThread(() -> {
            try {
                Duration duration = Duration.ofMillis(getMilliSeconds(timeout));
                ArrayType arrayType = (ArrayType) TypeUtils.getImpliedType(bTypedesc.getDescribingType());
                BArray dataArray = ValueCreator.createArrayValue(arrayType);
                boolean constraintValidation = (boolean) consumerObject.getMapValue(CONSUMER_CONFIG_FIELD_NAME)
                        .get(CONSTRAINT_VALIDATION);
                boolean autoCommit = getAutoCommitConfig(consumerObject);
                boolean autoSeek = getAutoSeekOnErrorConfig(consumerObject);
                ConsumerRecords recordsRetrieved;
                synchronized (kafkaConsumer) {
                    recordsRetrieved = kafkaConsumer.poll(duration);
                    if (!recordsRetrieved.isEmpty()) {
                        dataArray = getValuesWithIntendedType(consumerObject, arrayType, kafkaConsumer,
                                recordsRetrieved, constraintValidation, autoCommit, autoSeek);
                    }
                }
                balFuture.complete(dataArray);
            } catch (BError bError) {
                KafkaMetricsUtil.reportConsumerError(consumerObject, KafkaObservabilityConstants.ERROR_TYPE_POLL);
                balFuture.complete(bError);
            } catch (IllegalStateException | IllegalArgumentException | KafkaException e) {
                KafkaMetricsUtil.reportConsumerError(consumerObject, KafkaObservabilityConstants.ERROR_TYPE_POLL);
                String detailedError = getDetailedErrorMessage(e);
                logger.error("Failed to poll from Kafka server: {}", detailedError, e);
                balFuture.complete(createKafkaError("Failed to poll from the Kafka server: " + detailedError));
            }
        });
        return ModuleUtils.getResult(balFuture);
    }

    private static RecordType getRecordType(BTypedesc bTypedesc) {
        RecordType recordType;
        if (bTypedesc.getDescribingType().isReadOnly()) {
            recordType = (RecordType) getReferredType(((IntersectionType) getReferredType(((ArrayType)
                    TypeUtils.getImpliedType(bTypedesc.getDescribingType())).getElementType()))
                    .getConstituentTypes().get(0));
        } else {
            recordType = (RecordType) getReferredType(
                    ((ArrayType) getReferredType(bTypedesc.getDescribingType())).getElementType());
        }
        return recordType;
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

    static class KafkaThreadFactory implements ThreadFactory {

        @Override
        public Thread newThread(Runnable r) {
            Thread ballerinaKafka = new Thread(r);
            ballerinaKafka.setName("balx-kafka-consumer-network-thread");
            return ballerinaKafka;
        }
    }
}
