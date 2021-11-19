# Change Log
This file contains all the notable changes done to the Ballerina Kafka package through the releases.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- [Add Kafka listener detach functionality.](https://github.com/ballerina-platform/ballerina-standard-library/issues/2211)

### Changed
- [Mark Kafka Service type as distinct.](https://github.com/ballerina-platform/ballerina-standard-library/issues/2398)

## [3.0.0] - 2021-10-09

### Fixed
- [Fix issue in messages received to Kafka consumer services not being ordered.](https://github.com/ballerina-platform/ballerina-standard-library/issues/1698)
- [Fix issue in Kafka Consumer using Ballerina worker threads to do IO.](https://github.com/ballerina-platform/ballerina-standard-library/issues/1694)

## [2.1.0-beta.2] - 2021-07-05

### Added

- [Add public certificate and private key support for Kafka `SecureSocket` record.](https://github.com/ballerina-platform/ballerina-standard-library/issues/1469)

## [2.1.0-alpha8] - 2021-04-22

### Added

- [Add compiler plugin validations for Kafka services.](https://github.com/ballerina-platform/ballerina-standard-library/issues/1237)


## [2.1.0-alpha6] - 2021-04-02

### Changed
- [Redesign and update the `SecureSocket` record.](https://github.com/ballerina-platform/ballerina-standard-library/issues/1177)
- [Change the configurations required to initialize the producer and the consumer.](https://github.com/ballerina-platform/ballerina-standard-library/issues/1177)
- [Change `ProducerConfigurarion` in `kafka:Producer` init to an included record parameter.](https://github.com/ballerina-platform/ballerina-standard-library/issues/1177)
- [Change `ConsumerConfigurarion` in `kafka:Consumer` and `kafka:Listener` init to an included record parameter.](https://github.com/ballerina-platform/ballerina-standard-library/issues/1177)
- [Change all the time value arguments to Ballerina type `decimal`.](https://github.com/ballerina-platform/ballerina-standard-library/issues/1177)
