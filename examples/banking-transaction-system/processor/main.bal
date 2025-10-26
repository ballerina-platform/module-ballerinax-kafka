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
configurable string registryAPIKey = ?;
configurable string registryAPISecret = ?;

// Kafka Configuration
configurable string bootstrapServers = ?;
configurable string kafkaAPIKey = ?;
configurable string kafkaAPISecret = ?;
configurable string topicName = ?;
configurable string groupId = ?;

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

// Kafka Consumer Configuration
listener kafka:Listener transactionListener = new (bootstrapServers, {
    groupId: groupId,
    topics: [topicName],
    securityProtocol: kafka:PROTOCOL_SASL_SSL,
    auth: {
        mechanism: kafka:AUTH_SASL_PLAIN,
        username: kafkaAPIKey,
        password: kafkaAPISecret
    },
    schemaRegistryUrl: baseUrl,
    schemaRegistryConfig: {
        "baseUrl": baseUrl,
        "originals": {
            "basic.auth.credentials.source": "USER_INFO",
            "basic.auth.user.info": string `${registryAPIKey}:${registryAPISecret}`
        }
    },
    valueDeserializerType: kafka:DES_AVRO,
    offsetReset: kafka:OFFSET_RESET_EARLIEST
});

// Transaction Processing Service
service on transactionListener {

    remote function onConsumerRecord(Transaction[] transactions) returns error? {
        foreach Transaction txn in transactions {
            processTransaction(txn);
        }
    }
}

// Process individual transaction
function processTransaction(Transaction txn) {
    io:println("============================================================");
    io:println("🏦 PROCESSING TRANSACTION");
    io:println("============================================================");
    io:println("Transaction ID: ", txn.transaction_id);
    io:println("Account: ", txn.account_number);
    io:println("Type: ", txn.transaction_type);
    io:println("Amount: ", txn.amount.toString(), " ", txn.currency);
    io:println("Status: ", txn.status);
    io:println("Timestamp: ", txn.timestamp);
    io:println("Description: ", txn.description);

    // Validate business rules
    if txn.amount <= 0f {
        io:println("  ❌ ERROR: Invalid transaction - Amount must be positive");
        io:println("  📝 Transaction marked as FAILED");
        io:println("============================================================\n");
        return;
    }

    // Process based on transaction type
    match txn.transaction_type {
        DEPOSIT => {
            processDeposit(txn);
        }
        WITHDRAWAL => {
            processWithdrawal(txn);
        }
        TRANSFER => {
            processTransfer(txn);
        }
    }

    io:println("✅ TRANSACTION PROCESSED SUCCESSFULLY");
    io:println("============================================================\n");
}

// Process deposit transaction
function processDeposit(Transaction txn) {
    io:println("  💰 Processing deposit of ", txn.amount.toString(), " ", txn.currency);
    io:println("  ✓ Account ", txn.account_number, " credited");
    io:println("  📝 Transaction logged for compliance (ID: ", txn.transaction_id, ")");
}

// Process withdrawal transaction
function processWithdrawal(Transaction txn) {
    io:println("  💸 Processing withdrawal of ", txn.amount.toString(), " ", txn.currency);

    // Fraud detection for large withdrawals
    if txn.amount > 10000f {
        io:println("  ⚠️  ALERT: Large withdrawal detected (", txn.amount.toString(), " ", txn.currency, ")");
        io:println("  🚨 Triggering fraud detection workflow");
        io:println("  📧 Notification sent to account holder");
        io:println("  🔍 Transaction flagged for review");
    }

    io:println("  ✓ Account ", txn.account_number, " debited");
    io:println("  📝 Transaction logged for compliance (ID: ", txn.transaction_id, ")");
}

// Process transfer txn
function processTransfer(Transaction txn) {
    io:println("  🔄 Processing transfer of ", txn.amount.toString(), " ", txn.currency);

    // Validate recipient account
    if txn.recipient_account is () {
        io:println("  ❌ ERROR: Transfer requires recipient account");
        io:println("  📝 Transaction marked as FAILED");
        return;
    }

    string recipient = txn.recipient_account ?: "";
    io:println("  ✓ Source account ", txn.account_number, " debited");
    io:println("  ✓ Recipient account ", recipient, " credited");
    io:println("  📝 Transaction logged for compliance (ID: ", txn.transaction_id, ")");
}
