# Order Manager

## Overview
This usecase is inspired by the [How Kafka Can Make Microservice Planet a Better Place](https://dzone.com/articles/how-kafka-can-make-microservice-planet-better) article.
This consists of 3 services.

- **Order Service** - An `http` service with a `kafka' producer which publishes an order to a `kafka` topic when a request is received.
- **Order Processor** - A `kafka` listener which listens to the `kafka` topic and validates the order. If the order is successful, it is published to a new topic by a `kafka` producer.
- **Notification Service** - A `kafka` listener which listens to the successful orders and mocks sending a notification.

## Implementation

![Order Manager](topology.png)

#### Setting Up Kafka
1. [Install Kafka in your local machine](https://kafka.apache.org/downloads)
2. [Use Kafka with docker](https://hub.docker.com/r/confluentinc/cp-kafka/)

## Run the Example

First, clone this repository, and then, run the following commands in the given order to run this example in your local machine. Use separate terminals for each step.

1. Run the Order Service.
```sh
$ cd examples/order_manager/order_service
$ bal run
```
2. Run the Order processor.
```sh
$ cd examples/order_manager/order_processor
$ bal run
```
3. Run the Notification service.
```sh
$ cd examples/order_manager/notification_service
$ bal run
```
4. Send a request to the order service with order details using `curl` or using the browser.
   - Using curl - `curl -X GET "http://localhost:9090/kafka/publish?message=PS5&status=SUCCESS"`
   - Using browser - Launch the browser with the url `http://localhost:9090/kafka/publish?message=PS5&status=SUCCESS` 