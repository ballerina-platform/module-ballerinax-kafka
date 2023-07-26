# Tweet Analyzer

[![Star on Github](https://img.shields.io/badge/-Star%20on%20Github-blue?style=social&logo=github)](https://github.com/ballerina-platform/module-ballerinax-kafka)

_Authors_: @shafreenAnfar @aashikam  
_Reviewers_: @shafreenAnfar  
_Created_: 2021/08/17  
_Updated_: 2023/04/18

## Overview
This tweet analyzer application is implemented using the APIs provided by the `http` and `kafka` packages.

## Implementation

![Twitter Analyzer](twitter-analyzer.png)

### Mock Twitter Server

As it is strenuous to gain access to a Twitter developer account, a mock Twitter server is used to generate the tweets. Tweets containing random integers in the range 1 and 100000 will be generated. The response is a similar replica of the actual response given by the [Twitter Developer APIs](https://developer.twitter.com/en/docs/twitter-api/data-dictionary/object-model/tweet). 

### Twitter Client/ Kafka Producer

The Twitter clients can pull tweets by connecting and sending requests to the mock Twitter server. Once the response with the generated tweets is returned. The tweets are published to a Kafka topic using a Kafka producer. 

### Elasticsearch Client/ Kafka Consumer

The tweets published to the Kafka topic are consumed using a Ballerina Kafka consumer service. The received tweets with id numbers greater than 50000 are filtered. Finally, the filtered tweets are added to an Elasticsearch index. Elasticsearch is where the indexing, search, and analysis happens. The Elasticsearch client used here is implemented using an HTTP client with basic authentication.

#### Setting Up Elasticsearch
1. [Elasticsearch in your local machine](https://www.elastic.co/guide/en/elasticsearch/reference/current/setup.html)
2. [Elasticsearch in the cloud with Bonsai](https://bonsai.io/)

To jump-start with the example application, you can sign up to [Bonsai](https://bonsai.io/). By signing up, you will be provided with a free Elasticsearch sandbox cluster, the URL to connect, and the credentials for basic authentication.

This example can be used to connect to Twitter developer APIs. When used with Twitter developer APIs, this approach can be used to filter tweets with certain keywords. To apply for a Twitter developer account, go to the [Twitter Developer Platform](https://developer.twitter.com/en/apply-for-access). 

## Run the Example

First, clone this repository, and then, run the following commands in the given order to run this example in your local machine. Use separate terminals for each step.

1. Run the mock Twitter server. 
```sh
$ cd examples/twitter-filter/mock-twitter-server
$ bal run
```
2. Run the Elasticsearch Client / Kafka consumer.
```sh
$ cd examples/twitter-filter/elasticsearch-consumer
$ bal run
```
3. Run the Twitter Client / Kafka producer. 
```sh
$ cd examples/twitter-filter/twitter-producer
$ bal run
```

You can use the interactive console provided by [Bonsai](https://bonsai.io/) to search for the added tweets. 
