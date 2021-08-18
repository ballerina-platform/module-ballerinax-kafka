# Tweet Analyzer

## Overview
This tweet analyzer application is implemented using the APIs provided by the packages HTTP and Kafka.

## Implementation

![image](twitter-analyzer.png)

### Mock Twitter Server

As it is strenuous to gain access to a Twitter developer account, a mock Twitter server is used to generate tweets. Tweets are generated containing random integers in the range 1 and 100000.

### Twitter Client/ Kafka Producer

The Twitter clients can pull Tweets by connecting and sending requests to the mock Twitter server. Once the response with generated tweets is returned, the tweets with numbers greater than 50000 are filtered. Finally, the filtered tweets are published to a Kafka topic using a Kafka producer. 
### Elasticsearch Client/ Kafka Consumer

The tweets published to the Kafka topic are consumed using a Ballerina Kafka consumer service. The tweets received are then added to an Elasticsearch index. Elasticsearch is where the indexing, search, and analysis magic happens. The Elasticsearch client used here is implemented using an HTTP client with basic authentication.

#### Setting Up Elasticsearch
1. [Elasticsearch in your local machine](https://www.elastic.co/guide/en/elasticsearch/reference/current/setup.html)
2. [Elasticsearch in the cloud with Bonsai](https://bonsai.io/)

To jump-start with the example application, you can sign up to [Bonsai](https://bonsai.io/). By signing up you will be provided with a free Elasticsearch sandbox cluster, the URL to connect to, and the credentials for basic authentication.

This example can be used to connect to Twitter developer APIs. When used with Twitter developer APIs, this approach can be used to filter tweets with certain keywords. To apply for a Twitter developer account, go to [Twitter Developer Platform](https://developer.twitter.com/en/apply-for-access). 

## Run the Example

First, clone this repository, and then run the following commands in the given order, to run this example in your local machine. Use separate terminals for each step.

1. Run the mock Twitter server. 
```sh
$ cd examples/twitter-filter/mock_twitter_server
$ bal run
```
2. Run the Elasticsearch Client/ Kafka Consumer.
```sh
$ cd examples/twitter-filter/elasticsearch_consumer
$ bal run
```
3. Run the Twitter Client/ Kafka Producer. 
```sh
$ cd examples/twitter-filter/twitter_producer
$ bal run
```

You can use the interactive console provided by [Bonsai](https://bonsai.io/) to search added Tweets. 
