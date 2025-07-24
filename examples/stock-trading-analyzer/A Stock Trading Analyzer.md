# Stock Trading Analyzer

[![Star on GitHub](https://img.shields.io/badge/-Star%20on%20GitHub-blue?style=social&logo=github)](https://github.com/ballerina-platform/module-ballerinax-kafka)

_Authors_: @ThisaruGuruge
_Reviewers_: —
_Created_: 2024/07/24
_Updated_: 2024/07/24

## Overview

This example simulates a stock trading system using Kafka and Ballerina. It demonstrates how a Kafka producer sends mock trade events to a topic, and a Kafka consumer that filters the trades by a given timestamp and a given symbol.

## Implementation

- The **Trade Client** (`trade-client`) publishes mock stock trade events (symbol, price, quantity, etc.) to the sample REST endpoint.
- The **Trade Server** (`trade-server`) listens to the trade events and publishes them to the Kafka topic `trades`.
- The **Trade Analyzer** (`trade-analyzer`) is a Kafka consumer that filters the trades by a given timestamp and a given symbol.

## Setting Up Kafka

1. [Install Kafka locally](https://kafka.apache.org/downloads)
2. Or run Kafka using Docker:
   ```sh
   docker-compose -f ballerina/tests/resources/docker-compose.yaml up -d
````

This setup includes a basic Kafka broker and Zookeeper using the Confluent platform.

## Run the Example

First, clone the repository and navigate to the example directory. Then, in separate terminals:

1. **Start the Trade Server:**

   ```sh
   cd examples/stock-trading-analyzer/trade-api
   bal run
   ```

   This will start the Trade Server on port `9090`.

2. **Start the Trade Client:**

   ```sh
   cd examples/stock-trading-analyzer/trade-client
   bal run
   ```

   This will publish mock trade events to the Trade Server.

3. **Start the Trade Analyzer:**

   ```sh
   cd examples/stock-trading-analyzer/trade-analyzer
   bal run -- 2025-07-24T07:14:56.415548Z AAPL
   ```

   This will start the Trade Analyzer and filter the trades by the given timestamp and symbol.

Example output:

```
[INFO] Received trade: {"symbol":"AAPL", "price":187.45, "volume":100}
[INFO] Received trade: {"symbol":"GOOG", "price":2780.10, "volume":50}
```
