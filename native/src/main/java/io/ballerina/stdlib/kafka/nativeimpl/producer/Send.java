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

package io.ballerina.stdlib.kafka.nativeimpl.producer;

import io.ballerina.runtime.api.Environment;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.transactions.TransactionResourceManager;
import io.ballerina.stdlib.kafka.observability.KafkaMetricsUtil;
import io.ballerina.stdlib.kafka.observability.KafkaObservabilityConstants;
import io.ballerina.stdlib.kafka.observability.KafkaTracingUtil;
import io.ballerina.stdlib.kafka.utils.ModuleUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import static io.ballerina.stdlib.kafka.utils.KafkaConstants.NATIVE_PRODUCER;
import static io.ballerina.stdlib.kafka.utils.KafkaConstants.UNCHECKED;
import static io.ballerina.stdlib.kafka.utils.KafkaUtils.createKafkaError;
import static io.ballerina.stdlib.kafka.utils.TransactionUtils.handleTransactions;

/**
 * Native method to send different types of keys and values to kafka broker from ballerina kafka producer.
 */
public class Send {

    private static final ExecutorService executorService = Executors.newCachedThreadPool(new KafkaThreadFactory());

    @SuppressWarnings(UNCHECKED)
    protected static Object sendKafkaRecord(Environment env, ProducerRecord record, BObject producerObject) {
        KafkaTracingUtil.traceResourceInvocation(env, producerObject, record.topic());
        final CompletableFuture<Object> balFuture = new CompletableFuture<>();
        KafkaProducer producer = (KafkaProducer) producerObject.getNativeData(NATIVE_PRODUCER);
        Thread.startVirtualThread(() -> {
            if (TransactionResourceManager.getInstance().isInTransaction()) {
                handleTransactions(producerObject);
            }
            try {
                producer.send(record, (metadata, e) -> {
                    if (Objects.nonNull(e)) {
                        KafkaMetricsUtil.reportProducerError(producerObject,
                                KafkaObservabilityConstants.ERROR_TYPE_PUBLISH);
                        balFuture.complete(createKafkaError("Failed to send data to Kafka server: " + e.getMessage()));
                    } else {
                        KafkaMetricsUtil.reportPublish(producerObject, record.topic(), record.value());
                        balFuture.complete(null);
                    }
                });
            } catch (IllegalStateException | KafkaException e) {
                KafkaMetricsUtil.reportProducerError(producerObject, KafkaObservabilityConstants.ERROR_TYPE_PUBLISH);
                balFuture.complete(createKafkaError("Failed to send data to Kafka server: " + e.getMessage()));
            }
        });
        return ModuleUtils.getResult(balFuture);
    }

    static class KafkaThreadFactory implements ThreadFactory {

        @Override
        public Thread newThread(Runnable r) {
            Thread kafkaProducerThread = new Thread(r);
            kafkaProducerThread.setName("balx-kafka-producer-network-thread");
            return kafkaProducerThread;
        }
    }
}
