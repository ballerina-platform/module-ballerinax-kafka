# Specification: Ballerina RabbitMQ Library

_Owners_: @shafreenAnfar @dilanSachi    
_Reviewers_: @shafreenAnfar  
_Created_: 2020/10/28   
_Updated_: 2021/12/07  
_Issue_: [#2186](https://github.com/ballerina-platform/ballerina-standard-library/issues/2186)  

# Introduction
This is the specification for Kafka standard library which is used to send and receive messages by connecting to a Kafka server.
This library is programmed in the [Ballerina programming language](https://ballerina.io/), which is an open-source programming language for the cloud
that makes it easier to use, combine, and create network services.

# Contents

1. [Overview](#1-overview)
2. [Producer](#2-producer)
3. [Consumer](#3-consumer)
4. [Listener](#4-listener)

## 1. Overview

Apache Kafka is an open-source distributed event streaming platform for high-performance data pipelines, 
streaming analytics, data integration, and mission-critical applications. This specification elaborates on 
the usage of Kafka clients, producer and consumer. These clients allow writing distributed applications and 
microservices that read, write, and process streams of events in parallel, at scale, and in a fault-tolerant 
manner even in the case of network problems or machine failures.

Ballerina Kafka contains three core apis:
1. Producer
2. Consumer
3. Listener

## 2. Producer
### 2.1 Configurations
### 2.2 Connection
#### 2.2.1 Insecure Connection

#### 2.2.2 Secure Connection

### 2.3 Produce Messages

## 3. Consumer
### 3.1 Configurations
### 3.2 Connection
#### 3.2.1 Insecure Connection

#### 3.2.2 Secure Connection

### 3.3 Consumer Messages

