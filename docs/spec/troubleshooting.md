# Troubleshooting Guide: Ballerina Kafka Connector

_Authors_: @aashikam \
_Created_: 2025/11/27 \
_Updated_: 2025/12/11 \
_Edition_: Swan Lake

## Introduction

This guide helps you diagnose and resolve common issues when using the Ballerina Kafka connector (`ballerinax/kafka`). It covers connection problems, configuration errors, serialization issues, and runtime failures for both producers and consumers.

## Contents

1. [Connection Issues](#1-connection-issues)
   * 1.1. [Server Unavailable](#11-server-unavailable)
   * 1.2. [Connection Timeout](#12-connection-timeout)
   * 1.3. [DNS Resolution Failures](#13-dns-resolution-failures)
   * 1.4. [Consumer Already Connected](#14-consumer-already-connected)
2. [Authentication & Security Issues](#2-authentication--security-issues)
   * 2.1. [SASL Authentication Failures](#21-sasl-authentication-failures)
   * 2.2. [SSL/TLS Certificate Issues](#22-ssltls-certificate-issues)
   * 2.3. [Security Protocol Mismatch](#23-security-protocol-mismatch)
3. [Producer Issues](#3-producer-issues)
   * 3.1. [Failed to Send Data](#31-failed-to-send-data)
   * 3.2. [Serialization Errors](#32-serialization-errors)
   * 3.3. [Transaction Errors](#33-transaction-errors)
   * 3.4. [Buffer Memory Issues](#34-buffer-memory-issues)
   * 3.5. [Producer Flush Errors](#35-producer-flush-errors)
   * 3.6. [Producer Close Errors](#36-producer-close-errors)
   * 3.7. [Producer Initialization Errors](#37-producer-initialization-errors)
4. [Consumer Issues](#4-consumer-issues)
   * 4.1. [Failed to Poll](#41-failed-to-poll)
   * 4.2. [Consumer Group Issues](#42-consumer-group-issues)
   * 4.3. [Offset Management Issues](#43-offset-management-issues)
   * 4.4. [Deserialization Errors](#44-deserialization-errors)
   * 4.5. [Data Binding & Validation Errors](#45-data-binding--validation-errors)
   * 4.6. [Seek Errors](#46-seek-errors)
   * 4.7. [Pause and Resume Errors](#47-pause-and-resume-errors)
   * 4.8. [Subscription Errors](#48-subscription-errors)
   * 4.9. [Consumer Close Errors](#49-consumer-close-errors)
   * 4.10. [Assignment Errors](#410-assignment-errors)
5. [Listener Issues](#5-listener-issues)
   * 5.1. [Connection Unavailability in Services](#51-connection-unavailability-in-services)
   * 5.2. [Service Attachment Errors](#52-service-attachment-errors)
   * 5.3. [Compiler Plugin Validation Errors](#53-compiler-plugin-validation-errors)
   * 5.4. [Handling Errors in Listener](#54-handling-errors-in-listener)
   * 5.5. [Listener Stop Errors](#55-listener-stop-errors)
6. [Schema Registry Issues](#6-schema-registry-issues)
7. [Performance Issues](#7-performance-issues)
8. [Diagnostic Tools](#8-diagnostic-tools)

---

## 1. Connection Issues

### 1.1. Server Unavailable

**Error Message:**
```
Server might not be available at <bootstrap-servers>. No active connections found.
```

**Cause:**
The Kafka broker is not reachable. This error is raised when:
- The Kafka server is not running
- The server address/port is incorrect
- Network connectivity issues exist between the client and server
- Firewall rules are blocking the connection

Note: This error includes the bootstrap server address to help identify which server(s) are unavailable.

**Solution:**

1. **Verify Kafka server is running:**
   ```bash
   # Check if Kafka process is running
   ps aux | grep kafka

   # Or check if the port is listening
   netstat -an | grep 9092
   ```

2. **Verify the bootstrap server URL:**
   ```ballerina
   // Correct format: "host:port"
   kafka:Producer producer = check new ("localhost:9092");

   // For multiple brokers, use a string array
   kafka:Producer producer = check new (["broker1:9092", "broker2:9092", "broker3:9092"]);
   ```

3. **Test network connectivity:**
   ```bash
   # Test if you can reach the Kafka broker
   telnet localhost 9092

   # Or using netcat
   nc -zv localhost 9092
   ```

4. **Check firewall rules:**
   ```bash
   # On Linux
   sudo iptables -L -n | grep 9092

   # On macOS
   sudo pfctl -sr | grep 9092
   ```

**Example - Proper error handling:**
```ballerina
import ballerinax/kafka;
import ballerina/io;

public function main() returns error? {
    kafka:Producer producer = check new ("localhost:9092");

    kafka:Error? result = producer->send({
        topic: "my-topic",
        value: "Hello".toBytes()
    });

    if result is kafka:Error {
        string errorMsg = result.message();
        if errorMsg.includes("Server might not be available") {
            // Handle server unavailable scenario
            // Error includes bootstrap server address for easier debugging
            io:println("Kafka server is not available. Error: " + errorMsg);
        }
    }

    check producer->close();
}
```

### 1.2. Connection Timeout

**Error Message:**
```
Failed to poll from the Kafka server: Timeout expired while fetching topic metadata
```

**Cause:**
- Network latency is too high
- Kafka broker is overloaded
- `requestTimeout` or `defaultApiTimeout` is too short

**Solution:**

1. **Increase timeout values:**
   ```ballerina
   kafka:ConsumerConfiguration config = {
       groupId: "my-group",
       topics: ["my-topic"],
       requestTimeout: 60,        // 60 seconds (default: 30s)
       defaultApiTimeout: 120,    // 120 seconds
       sessionTimeout: 45,        // 45 seconds (default: 10s)
       connectionMaxIdleTime: 600 // 10 minutes
   };

   kafka:Consumer consumer = check new ("localhost:9092", config);
   ```

2. **For producers:**
   ```ballerina
   kafka:ProducerConfiguration config = {
       requestTimeout: 60,        // 60 seconds
       maxBlock: 120,             // Maximum time to block waiting for buffer space
       connectionsMaxIdleTime: 600
   };

   kafka:Producer producer = check new ("localhost:9092", config);
   ```

### 1.3. DNS Resolution Failures

**Error Message:**
```
Cannot connect to the kafka server: java.net.UnknownHostException: kafka-broker
```

**Cause:**
The hostname cannot be resolved to an IP address.

**Solution:**

1. **Use IP addresses directly for testing:**
   ```ballerina
   kafka:Producer producer = check new ("192.168.1.100:9092");
   ```

2. **Verify DNS resolution:**
   ```bash
   nslookup kafka-broker
   # or
   dig kafka-broker
   ```

3. **Add entries to `/etc/hosts` if needed:**
   ```
   192.168.1.100 kafka-broker
   ```

### 1.4. Consumer Already Connected

**Error Message:**
```
Kafka consumer is already connected to external broker. Please close it before re-connecting the external broker again.
```

**Cause:**
Attempting to reconnect a consumer that is already connected to a Kafka broker.

**Solution:**
```ballerina
// Close existing connection before reconnecting
check consumer->close();

// Then create a new consumer
kafka:Consumer newConsumer = check new ("localhost:9092", config);
```

---

## 2. Authentication & Security Issues

### 2.1. SASL Authentication Failures

**Error Message:**
```
Failed to initialize the producer: Authentication failed
```
```
SaslAuthenticationException: Authentication failed during authentication
```

**Cause:**
- Invalid username or password
- Wrong authentication mechanism configured
- Server doesn't support the configured mechanism

**Solution:**

1. **Verify credentials and mechanism:**
   ```ballerina
   kafka:AuthenticationConfiguration authConfig = {
       mechanism: kafka:AUTH_SASL_PLAIN,  // or AUTH_SASL_SCRAM_SHA_256, AUTH_SASL_SCRAM_SHA_512
       username: "your-username",
       password: "your-password"
   };

   kafka:ProducerConfiguration config = {
       auth: authConfig,
       securityProtocol: kafka:PROTOCOL_SASL_PLAINTEXT  // or PROTOCOL_SASL_SSL
   };

   kafka:Producer producer = check new ("localhost:9092", config);
   ```

2. **Available authentication mechanisms:**
   | Mechanism | Constant | Description |
   |-----------|----------|-------------|
   | PLAIN | `kafka:AUTH_SASL_PLAIN` | Simple username/password |
   | SCRAM-SHA-256 | `kafka:AUTH_SASL_SCRAM_SHA_256` | Salted Challenge Response |
   | SCRAM-SHA-512 | `kafka:AUTH_SASL_SCRAM_SHA_512` | Stronger SCRAM variant |

3. **Verify server configuration matches client:**
   Check your Kafka server's `server.properties`:
   ```properties
   sasl.enabled.mechanisms=PLAIN,SCRAM-SHA-256,SCRAM-SHA-512
   ```

### 2.2. SSL/TLS Certificate Issues

**Error Message:**
```
Error reading certificate file : <reason>
```
```
Error reading private key file : <reason>
```
```
SSLHandshakeException: PKIX path building failed
```

**Cause:**
- Certificate file not found or not readable
- Invalid certificate format
- Certificate chain incomplete
- Untrusted CA

**Solution:**

1. **Using TrustStore:**
   ```ballerina
   import ballerina/crypto;

   crypto:TrustStore trustStore = {
       path: "/path/to/truststore.p12",
       password: "truststore-password"
   };

   kafka:SecureSocket secureSocket = {
       cert: trustStore
   };

   kafka:ProducerConfiguration config = {
       secureSocket: secureSocket,
       securityProtocol: kafka:PROTOCOL_SSL
   };
   ```

2. **Using certificate files directly:**
   ```ballerina
   kafka:SecureSocket secureSocket = {
       cert: "/path/to/ca-cert.pem",
       key: {
           certFile: "/path/to/client-cert.pem",
           keyFile: "/path/to/client-key.pem",
           keyPassword: "key-password"  // if encrypted
       }
   };
   ```

3. **Verify certificate files:**
   ```bash
   # Check certificate
   openssl x509 -in /path/to/cert.pem -text -noout

   # Check private key
   openssl rsa -in /path/to/key.pem -check

   # Verify key matches certificate
   openssl x509 -noout -modulus -in cert.pem | openssl md5
   openssl rsa -noout -modulus -in key.pem | openssl md5
   ```

### 2.3. Security Protocol Mismatch

**Error Message:**
```
Connection to node -1 failed authentication due to: Unexpected handshake request
```

**Cause:**
Client security protocol doesn't match server configuration.

**Solution:**

Match the security protocol with your Kafka server:

| Protocol | Constant | Description |
|----------|----------|-------------|
| PLAINTEXT | `kafka:PROTOCOL_PLAINTEXT` | No authentication, no encryption |
| SASL_PLAINTEXT | `kafka:PROTOCOL_SASL_PLAINTEXT` | SASL authentication, no encryption |
| SSL | `kafka:PROTOCOL_SSL` | SSL encryption, optional client auth |
| SASL_SSL | `kafka:PROTOCOL_SASL_SSL` | SASL authentication + SSL encryption |

```ballerina
kafka:ProducerConfiguration config = {
    securityProtocol: kafka:PROTOCOL_SASL_SSL,
    auth: authConfig,
    secureSocket: secureSocket
};
```

---

## 3. Producer Issues

### 3.1. Failed to Send Data

**Error Message:**
```
Failed to send data to Kafka server: <reason>
```

**Common Causes & Solutions:**

1. **Topic doesn't exist (and auto-create is disabled):**
   ```bash
   # Create topic manually
   kafka-topics.sh --create --topic my-topic \
       --bootstrap-server localhost:9092 \
       --partitions 3 --replication-factor 1
   ```

2. **Message too large:**
   ```ballerina
   kafka:ProducerConfiguration config = {
       maxRequestSize: 10485760  // 10MB (default: 1MB)
   };
   ```

3. **Leader not available:**
   - Wait and retry
   - Check broker health
   - Verify topic has available replicas

4. **Producer closed:**
   Check if you're trying to send after calling `close()`:
   ```ballerina
   // Don't do this
   check producer->close();
   check producer->send({topic: "test", value: "data".toBytes()}); // Will fail
   ```

### 3.2. Serialization Errors

**Error Message:**
```
Failed to serialize key/value
```

**Cause:**
Data type mismatch between the value being sent and the configured serializer.

**Solution:**

1. **For byte array serialization (default):**
   ```ballerina
   // Value must be byte[]
   kafka:Error? result = producer->send({
       topic: "my-topic",
       value: "Hello World".toBytes()
   });
   ```

2. **For Avro serialization:**
   ```ballerina
   kafka:ProducerConfiguration config = {
       valueSerializerType: kafka:SER_AVRO,
       schemaRegistryConfig: {
           "baseUrl": "http://localhost:8081"
       },
       valueSchema: string `{"type":"record","name":"User","fields":[{"name":"name","type":"string"}]}`
   };

   kafka:Producer producer = check new ("localhost:9092", config);

   // Value can be a record matching the schema
   kafka:Error? result = producer->send({
       topic: "my-topic",
       value: {name: "John"}
   });
   ```

### 3.3. Transaction Errors

**Error Message:**
```
configuration enableIdempotence must be set to true to enable transactional producer
```

**Cause:**
Transactions require idempotence to be enabled.

**Solution:**
```ballerina
kafka:ProducerConfiguration config = {
    transactionalId: "my-transactional-id",
    enableIdempotence: true,  // Required for transactions
    acks: kafka:ACKS_ALL      // Recommended for transactions
};

kafka:Producer producer = check new ("localhost:9092", config);
```

### 3.4. Buffer Memory Issues

**Error Message:**
```
BufferExhaustedException: Failed to allocate memory within the configured max blocking time
```

**Cause:**
Producer buffer is full because messages are being produced faster than they can be sent.

**Solution:**
```ballerina
kafka:ProducerConfiguration config = {
    bufferMemory: 67108864,  // 64MB (default: 32MB)
    maxBlock: 120,           // Wait up to 120 seconds for buffer space
    batchSize: 32768,        // 32KB batch size
    linger: 0.005            // 5ms linger time
};
```

### 3.5. Producer Flush Errors

**Error Message:**
```
Failed to flush Kafka records: <reason>
```

**Cause:**
The producer failed to flush pending records to the broker.

**Solution:**
1. **Check broker connectivity** - Ensure the Kafka broker is running and accessible
2. **Handle flush errors gracefully:**
   ```ballerina
   kafka:Error? flushResult = producer->flushRecords();
   if flushResult is kafka:Error {
       // Some records may not have been sent
       log:printError("Failed to flush records", flushResult);
   }
   ```

### 3.6. Producer Close Errors

**Error Message:**
```
Failed to close the Kafka producer: <reason>
```

**Cause:**
Error occurred while closing the producer connection.

**Solution:**
```ballerina
kafka:Error? closeResult = producer->close();
if closeResult is kafka:Error {
    // Log but continue - producer may already be closed
    log:printWarn("Error closing producer", closeResult);
}
```

### 3.7. Producer Initialization Errors

**Error Message:**
```
Failed to initialize the producer: <reason>
```

**Cause:**
- Invalid configuration
- Authentication failure
- Network connectivity issues during initialization

**Solution:**
```ballerina
// Wrap producer creation in error handling
kafka:Producer|error producerResult = new ("localhost:9092", config);
if producerResult is error {
    log:printError("Failed to create producer", producerResult);
    // Handle initialization failure
}
```

---

## 4. Consumer Issues

### 4.1. Failed to Poll

**Error Message:**
```
Failed to poll from the Kafka server: <reason>
```

**Common Causes & Solutions:**

1. **Not subscribed to any topics:**
   ```ballerina
   kafka:ConsumerConfiguration config = {
       groupId: "my-group",
       topics: ["topic1", "topic2"]  // Subscribe during initialization
   };

   kafka:Consumer consumer = check new ("localhost:9092", config);

   // Or subscribe later
   check consumer->subscribe(["topic1", "topic2"]);
   ```

2. **Invalid topic name:**
   ```ballerina
   // Topic names should only contain:
   // - alphanumeric characters
   // - periods (.)
   // - underscores (_)
   // - hyphens (-)
   ```

3. **Consumer closed:**
   ```ballerina
   // Don't poll after closing
   check consumer->close();
   // consumer->poll(5); // This will fail!
   ```

### 4.2. Consumer Group Issues

**Error Message:**
```
Failed to assign topics for the consumer: The group member needs to have a valid member id
```
```
Member consumer-1 in group my-group has failed, removing it from the group
```

**Cause:**
- Session timeout exceeded
- Consumer took too long to process messages
- Network issues causing heartbeat failures

**Solution:**

1. **Adjust session and heartbeat settings:**
   ```ballerina
   kafka:ConsumerConfiguration config = {
       groupId: "my-group",
       topics: ["my-topic"],
       sessionTimeout: 45,       // Increase session timeout
       heartBeatInterval: 10,    // Heartbeat every 10 seconds
       maxPollInterval: 600,     // Max 10 minutes between polls
       maxPollRecords: 100       // Reduce records per poll if processing is slow
   };
   ```

2. **Ensure unique group IDs for different applications:**
   ```ballerina
   // Each consumer group should have a unique ID
   kafka:ConsumerConfiguration config = {
       groupId: "app1-consumer-group",  // Unique per application
       // ...
   };
   ```

### 4.3. Offset Management Issues

**Error Message:**
```
Failed to commit offsets: <reason>
```
```
Failed to commit the offset: <reason>
```

**Cause:**
- Consumer group rebalancing
- Broker unavailable
- Invalid offsets
- Consumer session expired

**Solution:**

1. **Auto-commit (default behavior):**
   ```ballerina
   kafka:ConsumerConfiguration config = {
       groupId: "my-group",
       topics: ["my-topic"],
       autoCommit: true,           // Default: true
       autoCommitInterval: 5       // Commit every 5 seconds
   };
   ```

2. **Manual commit:**
   ```ballerina
   kafka:ConsumerConfiguration config = {
       groupId: "my-group",
       topics: ["my-topic"],
       autoCommit: false
   };

   kafka:Consumer consumer = check new ("localhost:9092", config);

   kafka:BytesConsumerRecord[] records = check consumer->poll(10);
   // Process records...

   // Commit synchronously
   check consumer->'commit();

   // Or commit specific offsets
   check consumer->commitOffset([{
       partition: {topic: "my-topic", partition: 0},
       offset: 100
   }]);
   ```

3. **Handle offset reset:**
   ```ballerina
   kafka:ConsumerConfiguration config = {
       groupId: "my-group",
       topics: ["my-topic"],
       // When no committed offset exists:
       offsetReset: kafka:OFFSET_RESET_EARLIEST  // Start from beginning
       // or: kafka:OFFSET_RESET_LATEST          // Start from end
       // or: kafka:OFFSET_RESET_NONE            // Throw error
   };
   ```

### 4.4. Deserialization Errors

**Error Message:**
```
Failed to deserialize value
```

**Cause:**
Message format doesn't match the configured deserializer.

**Solution:**

1. **For byte array deserialization (default):**
   ```ballerina
   kafka:ConsumerConfiguration config = {
       groupId: "my-group",
       topics: ["my-topic"],
       keyDeserializerType: kafka:DES_BYTE_ARRAY,
       valueDeserializerType: kafka:DES_BYTE_ARRAY
   };

   kafka:Consumer consumer = check new ("localhost:9092", config);
   kafka:BytesConsumerRecord[] records = check consumer->poll(10);

   foreach var rec in records {
       string message = check string:fromBytes(rec.value);
   }
   ```

2. **For Avro deserialization:**
   ```ballerina
   kafka:ConsumerConfiguration config = {
       groupId: "my-group",
       topics: ["my-topic"],
       valueDeserializerType: kafka:DES_AVRO,
       schemaRegistryConfig: {
           "baseUrl": "http://localhost:8081"
       }
   };
   ```

### 4.5. Data Binding & Validation Errors

**Error Message:**
```
Data binding failed. If needed, please seek past the record to continue consumption.
```
```
Failed to validate payload. If needed, please seek past the record to continue consumption.
```

**Cause:**
- Message doesn't match the expected Ballerina type
- Constraint validation fails

**Solution:**

1. **Disable validation if not needed:**
   ```ballerina
   kafka:ConsumerConfiguration config = {
       groupId: "my-group",
       topics: ["my-topic"],
       validation: false  // Disable constraint validation
   };
   ```

2. **Auto-seek behavior (enabled by default):**

   By default, `autoSeekOnValidationFailure` is `true`, which means the consumer automatically seeks past records that fail data binding or validation. To disable this and handle errors manually:
   ```ballerina
   kafka:ConsumerConfiguration config = {
       groupId: "my-group",
       topics: ["my-topic"],
       validation: true,
       autoSeekOnValidationFailure: false  // Don't skip invalid records - handle in onError
   };
   ```

3. **Define proper types for data binding:**
   ```ballerina
   type OrderRecord record {|
       string orderId;
       string productName;
       int quantity;
       decimal price;
   |};

   // In listener service
   service on kafkaListener {
       remote function onConsumerRecord(OrderRecord[] orders) returns error? {
           // Process orders
       }
   }
   ```

### 4.6. Seek Errors

**Error Message:**
```
Failed to seek the consumer: <reason>
```
```
Failed to seek the consumer to the beginning: <reason>
```
```
Failed to seek the consumer to the end: <reason>
```

**Cause:**
- The partition is not assigned to this consumer
- Invalid offset value
- Consumer group rebalancing in progress

**Solution:**
1. **Ensure partitions are assigned before seeking:**
   ```ballerina
   // Get current assignment first
   kafka:TopicPartition[] assignment = check consumer->getAssignment();

   // Then seek on assigned partitions
   foreach var tp in assignment {
       check consumer->seek({
           partition: tp,
           offset: 0
       });
   }
   ```

2. **For seeking to beginning or end:**
   ```ballerina
   // Seek to beginning of all assigned partitions
   check consumer->seekToBeginning();

   // Or seek to end
   check consumer->seekToEnd();
   ```

### 4.7. Pause and Resume Errors

**Error Message:**
```
Failed to pause topic partitions for the consumer: <reason>
```
```
Failed to resume topic partitions for the consumer: <reason>
```

**Cause:**
- Attempting to pause/resume partitions not assigned to this consumer
- Consumer has been closed

**Solution:**
```ballerina
// Get assigned partitions
kafka:TopicPartition[] assigned = check consumer->getAssignment();

// Pause specific partitions
check consumer->pause(assigned);

// Later, resume them
check consumer->resume(assigned);

// Check which partitions are paused
kafka:TopicPartition[] paused = check consumer->getPausedPartitions();
```

### 4.8. Subscription Errors

**Error Message:**
```
Failed to subscribe to the provided topics: <reason>
```
```
Failed to unsubscribe the consumer: <reason>
```

**Cause:**
- Invalid topic names
- Consumer lacks permission to access topics
- Consumer already closed

**Solution:**
```ballerina
// Valid topic names contain: alphanumeric, periods, underscores, hyphens
check consumer->subscribe(["valid-topic-name", "another.topic"]);

// To unsubscribe
check consumer->unsubscribe();
```

### 4.9. Consumer Close Errors

**Error Message:**
```
Failed to close the connection from Kafka server: <reason>
```

**Cause:**
Error occurred while closing the consumer connection.

**Solution:**
```ballerina
kafka:Error? closeResult = consumer->close();
if closeResult is kafka:Error {
    log:printWarn("Error closing consumer", closeResult);
}
```

### 4.10. Assignment Errors

**Error Message:**
```
Failed to assign topics for the consumer: <reason>
```
```
Failed to retrieve assignment for the consumer: <reason>
```

**Cause:**
- Invalid topic partitions
- Consumer already closed
- Concurrent modification

**Solution:**
```ballerina
// Manually assign partitions (bypasses consumer group)
kafka:TopicPartition[] partitions = [
    {topic: "my-topic", partition: 0},
    {topic: "my-topic", partition: 1}
];
check consumer->assign(partitions);
```

---

## 5. Listener Issues

### 5.1. Connection Unavailability in Services

This is one of the most critical issues to handle in production Kafka services. When a Kafka broker becomes unavailable during service operation, the listener detects this and propagates the error to your service's `onError` function.

**Error Message:**
```
Server might not be available at <bootstrap-servers>. No active connections found.
```

**How It Works:**

The Kafka listener continuously polls the broker for new messages. When the poll returns empty results, the listener checks if there are any active connections to the broker. If no active connections are found, it indicates the server is unavailable and triggers the `onError` callback with the bootstrap server address included in the error message.

**Important Behavior:**
- The error includes the bootstrap server address (e.g., `localhost:9092`) to help identify which server(s) are unreachable
- If no `onError` function is implemented, the error is only printed to stderr and may go unnoticed
- After detecting server unavailability, the polling task continues attempting to reconnect
- Unrecoverable exceptions (like `KafkaException`, `IllegalStateException`) will cancel the polling task

**Solution - Implement Robust Error Handling:**

```ballerina
import ballerinax/kafka;
import ballerina/log;
import ballerina/lang.runtime;

kafka:ConsumerConfiguration consumerConfig = {
    groupId: "my-service-group",
    topics: ["my-topic"],
    pollingInterval: 1,       // Poll every 1 second
    sessionTimeout: 45,       // 45 seconds session timeout
    heartBeatInterval: 10,    // Heartbeat every 10 seconds
    maxPollInterval: 300,     // Max 5 minutes between polls
    reconnectBackoffTime: 1,  // Wait 1 second before reconnect attempt
    reconnectBackoffTimeMax: 60  // Max 60 seconds between reconnect attempts
};

listener kafka:Listener kafkaListener = new ("localhost:9092", consumerConfig);

service on kafkaListener {
    private boolean serverAvailable = true;

    remote function onConsumerRecord(kafka:BytesConsumerRecord[] records) returns error? {
        foreach var rec in records {
            // Process records
            string message = check string:fromBytes(rec.value);
            log:printInfo("Received: " + message);
        }
    }

    remote function onError(kafka:Error err, kafka:Caller caller) returns error? {
        string errorMsg = err.message();

        if errorMsg.includes("Server might not be available") {
            // Extract server address from error message for logging
            log:printError("CRITICAL: Kafka broker unavailable",
                'error = err,
                serverAddress = extractServerAddress(errorMsg));

            // Implement alerting (e.g., send to monitoring system)
            // alertMonitoringSystem("Kafka broker down: " + errorMsg);

            // Optionally trigger application-level recovery
            // The listener will continue attempting to reconnect automatically
        } else if errorMsg.includes("Data binding failed") {
            log:printWarn("Invalid message format, skipping record", 'error = err);
            // Data binding errors with autoSeekOnValidationFailure=true will auto-skip
        } else if errorMsg.includes("Failed to poll") {
            log:printError("Poll failure", 'error = err);
        } else {
            log:printError("Unexpected Kafka error", 'error = err);
        }
    }
}

function extractServerAddress(string errorMsg) returns string {
    // Error format: "Server might not be available at <address>. No active connections found."
    int? startIdx = errorMsg.indexOf("at ");
    int? endIdx = errorMsg.indexOf(". No active");
    if startIdx is int && endIdx is int {
        return errorMsg.substring(startIdx + 3, endIdx);
    }
    return "unknown";
}
```

**Best Practices for Production Services:**

1. **Always implement `onError`** - Without it, connection errors are silently printed to stderr
2. **Use the `kafka:Caller` parameter** - It gives you access to commit offsets manually during error recovery
3. **Configure appropriate timeouts:**
   ```ballerina
   kafka:ConsumerConfiguration config = {
       groupId: "my-group",
       topics: ["my-topic"],
       sessionTimeout: 45,           // Increase for unstable networks
       heartBeatInterval: 10,        // sessionTimeout / 3 is recommended
       reconnectBackoffTime: 1,      // Initial reconnect delay
       reconnectBackoffTimeMax: 60,  // Max reconnect delay (exponential backoff)
       requestTimeout: 60,           // Increase for slow networks
       connectionMaxIdleTime: 600    // Keep connections alive longer
   };
   ```
4. **Implement health checks** - Monitor the `onError` invocations
5. **Consider graceful degradation** - Have fallback behavior when Kafka is unavailable

**Connection Recovery Behavior:**

The Kafka listener has built-in reconnection logic:
- When server unavailability is detected, polling continues at the configured `pollingInterval`
- The underlying Kafka client uses exponential backoff between `reconnectBackoffTime` and `reconnectBackoffTimeMax`
- Once the broker becomes available again, normal message consumption resumes automatically
- No manual intervention is required for reconnection

**Detecting Intermittent vs Persistent Failures:**

```ballerina
service on kafkaListener {
    private int consecutiveErrors = 0;
    private final int MAX_CONSECUTIVE_ERRORS = 10;

    remote function onConsumerRecord(kafka:BytesConsumerRecord[] records) returns error? {
        // Reset error counter on successful processing
        self.consecutiveErrors = 0;
        // ... process records
    }

    remote function onError(kafka:Error err) returns error? {
        self.consecutiveErrors += 1;

        if err.message().includes("Server might not be available") {
            log:printError(string `Connection error (${self.consecutiveErrors}/${self.MAX_CONSECUTIVE_ERRORS})`,
                'error = err);

            if self.consecutiveErrors >= self.MAX_CONSECUTIVE_ERRORS {
                log:printError("Max consecutive errors reached - consider alerting operations team");
                // Trigger alert or take corrective action
            }
        }
    }
}
```

---

### 5.2. Service Attachment Errors

**Error Message:**
```
A service must be attached before starting the listener
```
```
A service must be attached before stopping the listener
```

**Cause:**
Attempting to start/stop a listener without attaching a service first.

**Solution:**
```ballerina
kafka:ConsumerConfiguration config = {
    groupId: "my-group",
    topics: ["my-topic"],
    pollingInterval: 1
};

kafka:Listener kafkaListener = check new ("localhost:9092", config);

// Attach service before starting
check kafkaListener.attach(myService);
check kafkaListener.'start();

// ... later ...

check kafkaListener.gracefulStop();
```

### 5.3. Compiler Plugin Validation Errors

The Kafka compiler plugin validates service implementations at compile time. Here are the possible validation errors:

| Error Code | Error Message | Solution |
|------------|---------------|----------|
| KAFKA_101 | Service must have remote method onConsumerRecord. | Add the `onConsumerRecord` remote function to your service |
| KAFKA_102 | Invalid remote method. | Only `onConsumerRecord` and `onError` remote methods are allowed |
| KAFKA_103 | Resource functions not allowed. | Use remote functions instead of resource functions |
| KAFKA_104 | Method must have the remote qualifier. | Add the `remote` keyword to your function |
| KAFKA_105 | Must have the required parameter kafka:AnydataConsumerRecord[] or anydata[] and optional parameter kafka:Caller. | Add the required records parameter |
| KAFKA_106 | Invalid method parameters. Only subtypes of kafka:AnydataConsumerRecord[], subtypes of anydata[] and kafka:Caller is allowed | Fix the parameter types |
| KAFKA_107 | Invalid method parameter. Only subtypes of kafka:AnydataConsumerRecord[] or subtypes of anydata[] is allowed. | Use the correct parameter type |
| KAFKA_108 | Invalid method parameter count. Only kafka:Caller and subtypes of kafka:AnydataConsumerRecord[] are allowed. | Reduce parameter count |
| KAFKA_109 | Invalid return type. Only error? or kafka:Error? is allowed. | Fix the return type |
| KAFKA_110 | Multiple listener attachments. Only one kafka:Listener is allowed. | Use only one Kafka listener per service |
| KAFKA_112 | Must have the required parameter kafka:Error | Add kafka:Error parameter to onError function |
| KAFKA_113 | Invalid method parameter. Only kafka:Error or error is allowed | Fix onError parameter type |
| KAFKA_114 | Invalid method parameter. Only kafka:Caller is allowed | Fix the second parameter type |

**Valid `onConsumerRecord` signatures:**
```ballerina
service on kafkaListener {
    // Option 1: Just records
    remote function onConsumerRecord(kafka:BytesConsumerRecord[] records) returns error? {
    }

    // Option 2: Records with caller
    remote function onConsumerRecord(kafka:BytesConsumerRecord[] records, kafka:Caller caller) returns error? {
    }

    // Option 3: With payload annotation for typed binding
    remote function onConsumerRecord(@kafka:Payload OrderRecord[] orders) returns error? {
    }
}
```

**Valid `onError` signature:**
```ballerina
service on kafkaListener {
    remote function onConsumerRecord(kafka:BytesConsumerRecord[] records) returns error? {
    }

    remote function onError(kafka:Error err) returns error? {
        // Handle errors
    }
}
```

### 5.4. Handling Errors in Listener

> **Note:** For comprehensive error handling patterns, especially for connection unavailability, see [Section 5.1: Connection Unavailability in Services](#51-connection-unavailability-in-services).

**Quick Reference - Common Error Patterns:**

```ballerina
service on kafkaListener {
    remote function onConsumerRecord(kafka:BytesConsumerRecord[] records) returns error? {
        foreach var rec in records {
            // Process each record
        }
    }

    remote function onError(kafka:Error err) returns error? {
        string errorMsg = err.message();

        // Connection errors - see section 5.1 for detailed handling
        if errorMsg.includes("Server might not be available") {
            log:printError("Kafka server unavailable", 'error = err);
        }
        // Data binding errors - consumer will auto-seek past bad records if autoSeekOnValidationFailure=true
        else if errorMsg.includes("Data binding failed") {
            log:printWarn("Failed to bind message data", 'error = err);
        }
        // Validation errors - handled similarly to data binding
        else if errorMsg.includes("Failed to validate payload") {
            log:printWarn("Payload validation failed", 'error = err);
        }
        // Poll failures
        else if errorMsg.includes("Failed to poll") {
            log:printError("Poll operation failed", 'error = err);
        }
        // Generic error handling
        else {
            log:printError("Kafka error occurred", 'error = err);
        }
    }
}
```

**Error Types Received by `onError`:**

| Error Pattern | Cause | Default Behavior |
|---------------|-------|------------------|
| "Server might not be available at..." | Broker unreachable | Continues polling, attempts reconnection |
| "Data binding failed..." | Message type mismatch | Auto-seeks past record if `autoSeekOnValidationFailure=true` |
| "Failed to validate payload..." | Constraint validation failed | Auto-seeks past record if `autoSeekOnValidationFailure=true` |
| "Failed to poll from the Kafka server..." | Various poll errors | Continues polling |
| KafkaException, IllegalStateException | Unrecoverable error | Cancels polling task |

### 5.5. Listener Stop Errors

**Error Message:**
```
Failed to stop the kafka service.
```

**Cause:**
An error occurred while stopping the Kafka listener service.

**Solution:**
```ballerina
kafka:Error? stopResult = kafkaListener.gracefulStop();
if stopResult is kafka:Error {
    // Log the error - listener may need to be recreated
    log:printError("Failed to stop listener gracefully", stopResult);
    // Consider recreating the listener if needed
}
```

---

## 6. Schema Registry Issues

**Error Message:**
```
Error occurred while initializing the confluent registry: <reason>
```
```
The provided values for 'schemaRegistryConfig' do not match the expected schema registry properties: <reason>
```

**Cause:**
- Schema registry URL is incorrect
- Authentication issues with schema registry
- Schema not found or incompatible

**Solution:**

1. **Basic schema registry configuration:**
   ```ballerina
   kafka:ProducerConfiguration config = {
       valueSerializerType: kafka:SER_AVRO,
       schemaRegistryConfig: {
           "baseUrl": "http://localhost:8081"
       }
   };
   ```

2. **With authentication:**
   ```ballerina
   kafka:ProducerConfiguration config = {
       valueSerializerType: kafka:SER_AVRO,
       schemaRegistryConfig: {
           "baseUrl": "http://localhost:8081",
           "basicAuth.credentialsSource": "USER_INFO",
           "basicAuth.userInfo": "username:password"
       }
   };
   ```

3. **Verify schema registry is accessible:**
   ```bash
   curl http://localhost:8081/subjects
   ```

---

## 7. Performance Issues

### Slow Consumer

**Symptoms:**
- Consumer lag increasing
- Consumer group rebalancing frequently
- Timeouts during poll

**Solutions:**

1. **Increase parallelism:**
   ```ballerina
   kafka:ConsumerConfiguration config = {
       groupId: "my-group",
       topics: ["my-topic"],
       concurrentConsumers: 4  // Process with 4 concurrent consumers
   };
   ```

2. **Optimize polling:**
   ```ballerina
   kafka:ConsumerConfiguration config = {
       groupId: "my-group",
       topics: ["my-topic"],
       maxPollRecords: 500,      // Fetch more records per poll
       fetchMinBytes: 1048576,   // Wait for 1MB before returning
       fetchMaxWaitTime: 0.5     // Max wait 500ms
   };
   ```

3. **Decouple processing:**
   ```ballerina
   kafka:ConsumerConfiguration config = {
       groupId: "my-group",
       topics: ["my-topic"],
       decoupleProcessing: true  // Process records asynchronously
   };
   ```

### Slow Producer

**Solutions:**

1. **Enable batching:**
   ```ballerina
   kafka:ProducerConfiguration config = {
       batchSize: 65536,    // 64KB batch
       linger: 0.005,       // Wait 5ms to batch
       compressionType: kafka:COMPRESSION_LZ4
   };
   ```

2. **Reduce acknowledgment requirements (trade-off with durability):**
   ```ballerina
   kafka:ProducerConfiguration config = {
       acks: kafka:ACKS_SINGLE  // Only wait for leader ack
       // or: kafka:ACKS_NONE   // Fire and forget (not recommended)
   };
   ```

---

## 8. Diagnostic Tools

### Enable Debug Logging

Add to your Ballerina application's `Config.toml`:
```toml
[ballerina.log]
level = "DEBUG"

[ballerinax.kafka]
level = "DEBUG"
```

### Kafka Command-Line Tools

```bash
# List topics
kafka-topics.sh --list --bootstrap-server localhost:9092

# Describe topic
kafka-topics.sh --describe --topic my-topic --bootstrap-server localhost:9092

# List consumer groups
kafka-consumer-groups.sh --list --bootstrap-server localhost:9092

# Describe consumer group (check lag)
kafka-consumer-groups.sh --describe --group my-group --bootstrap-server localhost:9092

# Reset offsets
kafka-consumer-groups.sh --reset-offsets --group my-group \
    --topic my-topic --to-earliest --execute \
    --bootstrap-server localhost:9092
```

### Check Broker Health

```bash
# Check broker status
kafka-broker-api-versions.sh --bootstrap-server localhost:9092

# Check cluster metadata
kafka-metadata.sh --snapshot /path/to/kafka-logs/__cluster_metadata-0/00000000000000000000.log --command describe
```

---

## Quick Reference: Common Errors

| Error Message | Likely Cause | Quick Fix |
|-----------|--------------|-----------|
| "Server might not be available at \<host\>" | Kafka broker down or unreachable | Check broker; implement `onError` in services (see 5.1) |
| "No active connections found" | Lost connection to Kafka | Verify server is running and accessible |
| "Failed to initialize the producer" | Invalid config or auth failure | Check configuration and credentials |
| "Failed to send data to Kafka server" | Various | Check topic exists, message size, permissions |
| "Failed to poll from the Kafka server" | Subscription or connection issue | Verify topic subscription |
| "Failed to commit offsets" | Consumer group rebalancing | Increase sessionTimeout |
| "Failed to seek the consumer" | Partition not assigned | Verify partition assignment |
| "Failed to pause/resume topic partitions" | Partition not assigned | Check partition assignment |
| "Cannot connect to the kafka server" | Connection failure | Check network and server status |
| "configuration enableIdempotence must be set to true" | Transaction misconfiguration | Set enableIdempotence: true |
| "Kafka consumer is already connected" | Reconnection without close | Close consumer before reconnecting |
| "A service must be attached" | Missing service attachment | Call attach() before start() |
| "Failed to stop the kafka service" | Listener stop failure | Check logs, recreate if needed |
| "Data binding failed" | Type mismatch | Fix payload type (autoSeekOnValidationFailure=true by default) |
| "Failed to validate payload" | Constraint validation failed | Fix data or set validation=false |
| "Error reading certificate file" | SSL misconfiguration | Verify certificate paths and format |
| "The provided values for 'schemaRegistryConfig'" | Schema registry config error | Use "baseUrl" key in schemaRegistryConfig |

> **Important for Services:** Always implement the `onError` remote function in your Kafka service to handle connection errors. Without it, errors are only printed to stderr and may go unnoticed. See [Section 5.1](#51-connection-unavailability-in-services) for detailed guidance.

---

## Getting Help

If you encounter issues not covered in this guide:

1. **Check the official documentation:** [Ballerina Kafka Module](https://central.ballerina.io/ballerinax/kafka/latest)

2. **Report issues:** [GitHub Issues](https://github.com/ballerina-platform/ballerina-standard-library/issues)

3. **Community support:** [Ballerina Discord](https://discord.gg/ballerinalang)
