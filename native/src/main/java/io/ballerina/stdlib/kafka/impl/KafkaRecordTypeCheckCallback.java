/*
 * Copyright (c) 2022, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

import io.ballerina.runtime.api.async.Callback;

import java.util.concurrent.Semaphore;

/**
 * {@code KafkaRecordTypeCheckCallback} provides ability to check whether a given type is a subtype of
 * kafka:AnydataConsumerRecord.
 */
public class KafkaRecordTypeCheckCallback implements Callback {

    private final Semaphore semaphore;
    private Boolean isConsumerRecordType = false;

    public KafkaRecordTypeCheckCallback(Semaphore semaphore) {
        this.semaphore = semaphore;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void notifySuccess(Object obj) {
        isConsumerRecordType = (Boolean) obj;
        semaphore.release();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void notifyFailure(io.ballerina.runtime.api.values.BError error) {
        semaphore.release();
        error.printStackTrace();
    }

    public boolean getIsConsumerRecordType () {
        return isConsumerRecordType;
    }
}
