# Change Log
This file contains all the notable changes done to the Ballerina Kafka package through the releases.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

### Added
- [Add SASL-SCRAM-256 authentication support](https://github.com/ballerina-platform/ballerina-library/issues/6766)

## [4.0.0] - 2024-05-05

### Removed
- [Removed deprecated `kafka:ConsumerRecord` and `kafka:ProducerRecord`](https://github.com/ballerina-platform/ballerina-library/issues/6359)

## [3.10.2] - 2024-03-28

### Fixed
- [Fixed api docs generation issue](https://github.com/ballerina-platform/ballerina-library/issues/6250)

## [3.10.1] - 2024-03-22

### Added
- [Added header support for Consumr and Producer](https://github.com/ballerina-platform/ballerina-library/issues/6196)

## [3.10.0] - 2023-09-18

### Changed
- [Updated Apache Kafka client version to `3.5.1`](https://github.com/ballerina-platform/ballerina-standard-library/issues/4752)

### Fixed
- [Removed `client` keyword from the `kafka:Listener`](https://github.com/ballerina-platform/ballerina-standard-library/issues/4750)

## [3.9.1] - 2023-08-18

### Changed
- [Changed disallowing service level annotations in the compiler plugin](https://github.com/ballerina-platform/ballerina-standard-library/issues/4731)

## [3.8.0] - 2023-06-01

### Fixed
- [Fixed log manager getting reset by `kafka` module](https://github.com/ballerina-platform/ballerina-standard-library/issues/4493)

## [3.7.1] - 2023-05-09

### Fixed
- [Fixed `ClassCastException` when doing `commitOffset` with a polled offset value](https://github.com/ballerina-platform/ballerina-standard-library/issues/4424)

## [3.7.0] - 2023-04-10
### Changed
- [Exit the service when a panic occurs inside the service method](https://github.com/ballerina-platform/ballerina-standard-library/issues/4241)

## [3.6.0] - 2023-02-20

### Added
- [Added a feature to autoseek when a payload binding fails or constraint validation fails](https://github.com/ballerina-platform/ballerina-standard-library/issues/3827)

### Changed
- [Stopped Java client logs from appearing in the console](https://github.com/ballerina-platform/ballerina-standard-library/issues/3698)

### Fixed
- [Fixed consumer panic when a type casting error occurs](https://github.com/ballerina-platform/ballerina-standard-library/issues/3696)
- [Fixed not logging errors returned from the kafka `onConsumerRecord` method](https://github.com/ballerina-platform/ballerina-standard-library/issues/3884)

## [3.5.1] - 2023-01-27
### Changed
- [Changed the send operation of `kafka:Producer` to be non-blocking](https://github.com/ballerina-platform/ballerina-standard-library/issues/3991)

## [3.5.0] - 2022-11-29

### Changed
- [Updated API Docs](https://github.com/ballerina-platform/ballerina-standard-library/issues/3463)
- [Added support for string type for topic config](https://github.com/ballerina-platform/ballerina-standard-library/issues/3695)

## [3.4.0] - 2022-09-08

### Added
- [Added constraint validation support for payload binding](https://github.com/ballerina-platform/ballerina-standard-library/issues/3053)

### Fixed
- [Fixed `NullPointerException` when setting `pollingTimeout` in `kafka:ConsumerConfiguration`](https://github.com/ballerina-platform/ballerina-standard-library/issues/3235)

## [3.3.0] - 2022-05-30

### Added
- [Added data binding support.](https://github.com/ballerina-platform/ballerina-standard-library/issues/2751)

## [3.2.0] - 2022-01-29

### Changed
- [Make kafka:Caller optional in onConsumerRecord method in kafka:Service.](https://github.com/ballerina-platform/ballerina-standard-library/issues/1661)

## [3.1.0] - 2021-10-09

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
