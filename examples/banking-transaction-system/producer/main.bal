// Copyright (c) 2025, WSO2 LLC. (http://www.wso2.org).
//
// WSO2 LLC. licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except
// in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

import ballerina/io;
import ballerinax/kafka;

// Schema Registry Configuration
configurable string baseUrl = ?;
configurable int identityMapCapacity = ?;
configurable map<anydata> & readonly originals = ?;

// Kafka Configuration
configurable string bootstrapServers = ?;
configurable string kafkaUsername = ?;
configurable string kafkaPassword = ?;
configurable string topicName = ?;

// Transaction Status Enum
public enum TransactionStatus {
    PENDING,
    COMPLETED,
    FAILED,
    CANCELLED
}

// Transaction Type Enum
public enum TransactionType {
    DEPOSIT,
    WITHDRAWAL,
    TRANSFER
}

// Transaction Record
type Transaction record {
    string transaction_id;
    string account_number;
    TransactionType transaction_type;
    float amount;
    string currency;
    string timestamp;
    TransactionStatus status;
    string description;
    string? recipient_account;
};

public function main() returns error? {
    io:println("🏦 Banking Transaction Producer Started");
    io:println("=====================================\n");

    // Produce sample transactions
    check produceTransactions();

    io:println("\n✅ All transactions sent successfully!");
    io:println("Schema was validated for each transaction before sending.\n");
    io:println("🔒 Producer closed.");
}

// Produce sample transactions to Kafka
function produceTransactions() returns error? {
    // Define the Avro schema for value serialization
    string valueSchema = string `{
        "namespace": "banking.avro",
        "type": "record",
        "name": "Transaction",
        "fields": [
            {"name": "transaction_id", "type": "string"},
            {"name": "account_number", "type": "string"},
            {"name": "transaction_type", "type": "string"},
            {"name": "amount", "type": {"type": "double"}},
            {"name": "currency", "type": "string"},
            {"name": "timestamp", "type": "string"},
            {"name": "status", "type": "string"},
            {"name": "description", "type": "string"},
            {"name": "recipient_account", "type": ["null", "string"], "default": null}
        ]
    }`;

    // Configure Kafka producer with Avro serialization
    kafka:ProducerConfiguration config = {
        compressionType: kafka:COMPRESSION_SNAPPY,
        auth: {
            username: kafkaUsername,
            password: kafkaPassword
        },
        securityProtocol: kafka:PROTOCOL_SASL_SSL,
        valueSerializerType: kafka:SER_AVRO,
        valueSchema: valueSchema,
        schemaRegistryConfig: {
            "baseUrl": baseUrl,
            "originals": originals
        }
    };

    kafka:Producer producer = check new (bootstrapServers, config);

    // Scenario 1: Deposit Transaction
    io:println("📥 Scenario 1: Processing DEPOSIT transaction...");
    Transaction deposit = {
        transaction_id: "550e8400-e29b-41d4-a716-446655440000",
        account_number: "ACC-123456",
        transaction_type: DEPOSIT,
        amount: 5000.00,
        currency: "USD",
        timestamp: "2025-10-25T12:30:00Z",
        status: PENDING,
        description: "Salary deposit",
        recipient_account: ()
    };
    check producer->send({topic: topicName, value: deposit});
    printTransactionSummary(deposit);

    // Scenario 2: Small Withdrawal Transaction
    io:println("💸 Scenario 2: Processing WITHDRAWAL transaction...");
    Transaction withdrawal = {
        transaction_id: "550e8400-e29b-41d4-a716-446655440001",
        account_number: "ACC-123456",
        transaction_type: WITHDRAWAL,
        amount: 200.50,
        currency: "USD",
        timestamp: "2025-10-25T12:30:15Z",
        status: PENDING,
        description: "ATM withdrawal",
        recipient_account: ()
    };
    check producer->send({topic: topicName, value: withdrawal});
    printTransactionSummary(withdrawal);

    // Scenario 3: Transfer Transaction
    io:println("🔄 Scenario 3: Processing TRANSFER transaction...");
    Transaction transfer = {
        transaction_id: "550e8400-e29b-41d4-a716-446655440002",
        account_number: "ACC-123456",
        transaction_type: TRANSFER,
        amount: 1500.00,
        currency: "USD",
        timestamp: "2025-10-25T12:30:30Z",
        status: PENDING,
        description: "Transfer to savings account",
        recipient_account: "ACC-789012"
    };
    check producer->send({topic: topicName, value: transfer});
    printTransactionSummary(transfer);

    // Scenario 4: Large Withdrawal (for fraud detection)
    io:println("🚨 Scenario 4: Processing LARGE WITHDRAWAL (potential fraud alert)...");
    Transaction largeWithdrawal = {
        transaction_id: "550e8400-e29b-41d4-a716-446655440003",
        account_number: "ACC-123456",
        transaction_type: WITHDRAWAL,
        amount: 15000.00,
        currency: "USD",
        timestamp: "2025-10-25T12:30:45Z",
        status: PENDING,
        description: "Large cash withdrawal",
        recipient_account: ()
    };
    check producer->send({topic: topicName, value: largeWithdrawal});
    printTransactionSummary(largeWithdrawal);

    check producer->'flush();
}

// Helper function to print transaction summary
function printTransactionSummary(Transaction txn) {
    io:println("  ✓ Transaction ID: ", txn.transaction_id);
    io:println("  ✓ Type: ", txn.transaction_type);
    io:println("  ✓ Amount: ", txn.amount.toString(), " ", txn.currency);
    io:println("  ✓ Account: ", txn.account_number);
    if txn.recipient_account is string {
        io:println("  ✓ Recipient: ", txn.recipient_account);
    }
    io:println("  ✓ Schema validated and sent to Kafka\n");
}
