package io.ballerina.stdlib.kafka.service;

import io.ballerina.runtime.api.Environment;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.stdlib.kafka.api.KafkaServerConnector;
import io.ballerina.stdlib.kafka.exceptions.KafkaConnectorException;
import io.ballerina.stdlib.kafka.impl.KafkaServerConnectorImpl;
import io.ballerina.stdlib.kafka.utils.KafkaUtils;

import static io.ballerina.stdlib.kafka.utils.KafkaConstants.SERVER_CONNECTOR;
import static io.ballerina.stdlib.kafka.utils.KafkaConstants.UNCHECKED;

/**
 * This is used to unregister a listener to the kafka service.
 */
public class Unregister {
    @SuppressWarnings(UNCHECKED)
    public static Object unregister(Environment env, BObject listener, BObject service) {
        KafkaServerConnector serverConnector = (KafkaServerConnectorImpl) listener.getNativeData(SERVER_CONNECTOR);
        try {
            serverConnector.stopPollingTask();
        } catch (KafkaConnectorException e) {
            return KafkaUtils.createKafkaError(e.getMessage());
        }
        return null;
    }
}
