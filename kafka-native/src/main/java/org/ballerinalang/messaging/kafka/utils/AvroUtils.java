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

package org.ballerinalang.messaging.kafka.utils;

import io.ballerina.runtime.api.utils.StringUtils;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BString;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;

import java.util.List;

/**
 * Utility functions to handle kafka avro operations.
 */
public class AvroUtils {
    /*
     * This is separated from the other classes since we don't pack avro dependencies  with the distribution.
     */
    private AvroUtils(){}

    protected static void populateBallerinaGenericAvroRecord(BMap<BString, Object> genericAvroRecord,
                                                             GenericRecord record) {
        List<Schema.Field> fields = record.getSchema().getFields();
        for (Schema.Field field : fields) {
            if (record.get(field.name()) instanceof Utf8) {
                genericAvroRecord.put(StringUtils.fromString(field.name()),
                                      StringUtils.fromString(record.get(field.name()).toString()));
            } else if (record.get(field.name()) instanceof GenericRecord) {
                populateBallerinaGenericAvroRecord(genericAvroRecord, (GenericRecord) record.get(field.name()));
            } else {
                genericAvroRecord.put(StringUtils.fromString(field.name()), record.get(field.name()));
            }
        }
    }

    protected static BMap<BString, Object> handleAvroConsumer(Object value) {
        if (value instanceof GenericRecord) {
            BMap<BString, Object> genericAvroRecord = KafkaUtils.getAvroGenericRecord();
            populateBallerinaGenericAvroRecord(genericAvroRecord, (GenericRecord) value);
            return genericAvroRecord;
        } else {
            throw KafkaUtils.createKafkaError("Invalid type - expected: AvroGenericRecord");
        }
    }
}
