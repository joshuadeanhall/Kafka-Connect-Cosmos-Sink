# Kafka Connect Azure Cosmos DB Change Feed


[![Build Status](https://travis-ci.org/joshuadeanhall/Kafka-Connect-Cosmos-Sink.svg?branch=master)](https://travis-ci.org/joshuadeanhall/Kafka-Connect-Cosmos-Sink)

This connector is used to load data from Azure Cosmos DB to Kafka using the [Change Feed](https://docs.microsoft.com/en-us/azure/cosmos-db/change-feed) functionality.

#TODO
* Testing
    1. Unit Test
    2. Kafka
* Look into partitions being created and rebalanced
* Switch partition readers to be multi threaded
* Improve offset setting
* Add Logging
* Improve readme with instructions for simple setup.
