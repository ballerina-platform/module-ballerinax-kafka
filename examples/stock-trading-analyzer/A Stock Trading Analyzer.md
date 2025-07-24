# Stock Trading Analyzer

[![Star on GitHub](https://img.shields.io/badge/-Star%20on%20GitHub-blue?style=social&logo=github)](https://github.com/ballerina-platform/module-ballerinax-kafka)

_Authors_: @ThisaruGuruge
_Reviewers_: —
_Created_: 2024/07/24
_Updated_: 2024/07/24

## Overview

This example simulates a stock trading system using Kafka and Ballerina. It demonstrates how a Kafka producer sends mock trade events to a topic, and a Kafka consumer listens to this topic to analyze trading activity and print a summary.

This use case is inspired by real-time processing requirements in stock exchanges, portfolio trackers, and algorithmic trading systems. It highlights the ease of working with Kafka in Ballerina for event-driven applications.

## Implementation

- The **Trade Client** (`trade-client`) publishes mock stock trade events (symbol, price, quantity, etc.) to the Kafka topic `trades`.
- The **Trade Server** (`trade-server`) listens to the `trades` topic and processes incoming events, logging trade data to the console.
- The **Trade Analyzer** (`trade-analyzer`) is a simple HTTP server that exposes a REST API to get the trade summary.

## Setting Up Kafka

1. [Install Kafka locally](https://kafka.apache.org/downloads)
2. Or run Kafka using Docker:
   ```sh
   docker-compose -f ballerina/tests/resources/docker-compose.yaml up -d
````

This setup includes a basic Kafka broker and Zookeeper using the Confluent platform.

## Run the Example

First, clone the repository and navigate to the example directory. Then, in separate terminals:

1. **Start the Kafka Consumer (Analyzer):**

   ```sh
   cd examples/stock_trading_analyzer/stock_trading_consumer
   bal run
   ```

2. **Start the Kafka Producer (Generator):**

   ```sh
   cd examples/stock_trading_analyzer/stock_trading_producer
   bal run
   ```

You will see mock trade events being sent and analyzed in real-time.

Example output:

```
[INFO] Received trade: {"symbol":"AAPL", "price":187.45, "volume":100}
[INFO] Received trade: {"symbol":"GOOG", "price":2780.10, "volume":50}
```
