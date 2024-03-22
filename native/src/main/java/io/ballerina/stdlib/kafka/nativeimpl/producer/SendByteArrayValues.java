/*
 *  Copyright (c) 2020 WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 *  WSO2 Inc. licenses this file to you under the Apache License,
 *  Version 2.0 (the "License"); you may not use this file except
 *  in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package io.ballerina.stdlib.kafka.nativeimpl.producer;

import io.ballerina.runtime.api.Environment;
import io.ballerina.runtime.api.values.BArray;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.api.values.BString;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static io.ballerina.stdlib.kafka.utils.KafkaConstants.ALIAS_PARTITION;
import static io.ballerina.stdlib.kafka.utils.KafkaUtils.getIntValue;
import static io.ballerina.stdlib.kafka.utils.KafkaUtils.getLongValue;

/**
 * Native methods to send {@code byte[]} values and with different types of keys to Kafka broker from ballerina kafka
 * producer.
 */
public class SendByteArrayValues extends Send {
    /* ************************************************************************ *
     *              Send records with value of type byte[]                      *
     *       The value is considered first since key can be null                *
     ************************************************************************** */

    private static final Logger logger = LoggerFactory.getLogger(SendByteArrayValues.class);

    // ballerina byte[]
    public static Object sendByteArrayValuesNilKeys(Environment env, BObject producer, BArray value, BString topic,
                                                    Object partition, Object timestamp, BArray headerList) {
        Integer partitionValue = getIntValue(partition, ALIAS_PARTITION, logger);
        Long timestampValue = getLongValue(timestamp);
        List<Header> headers = getHeadersFromBHeaders(headerList);
        ProducerRecord<?, byte[]> kafkaRecord = new ProducerRecord<>(topic.getValue(), partitionValue, timestampValue,
                null, value.getBytes(), headers);
        return sendKafkaRecord(env, kafkaRecord, producer);
    }

    // ballerina byte[] and ballerina byte[]
    public static Object sendByteArrayValuesByteArrayKeys(Environment env, BObject producer, BArray value,
                                                          BString topic, BArray key, Object partition,
                                                          Object timestamp, BArray headerList) {
        Integer partitionValue = getIntValue(partition, ALIAS_PARTITION, logger);
        Long timestampValue = getLongValue(timestamp);
        List<Header> headers = getHeadersFromBHeaders(headerList);
        ProducerRecord<byte[], byte[]> kafkaRecord = new ProducerRecord<>(topic.getValue(), partitionValue,
                timestampValue, key.getBytes(), value.getBytes(), headers);
        return sendKafkaRecord(env, kafkaRecord, producer);
    }

    private static List<Header> getHeadersFromBHeaders(BArray headerList) {
        List<Header> headers = new ArrayList<>();
        for (int i = 0; i < headerList.size(); i++) {
            BArray headerItem = (BArray) headerList.get(i);
            headers.add(new RecordHeader(headerItem.getBString(0).getValue(), ((BArray) headerItem.get(1)).getByteArray()));
        }
        return headers;
    }
}
