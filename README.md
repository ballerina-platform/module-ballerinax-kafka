# Ballerina Kafka Module


[![Build](https://github.com/ballerina-platform/module-ballerinax-kafka/actions/workflows/build-timestamped-master.yml/badge.svg)](https://github.com/ballerina-platform/module-ballerinax-kafka/actions/workflows/build-timestamped-master.yml)
[![GitHub Last Commit](https://img.shields.io/github/last-commit/ballerina-platform/module-ballerinax-kafka.svg)](https://github.com/ballerina-platform/module-ballerinax-kafka/commits/master)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![codecov](https://codecov.io/gh/ballerina-platform/module-ballerinax-kafka/branch/master/graph/badge.svg)](https://codecov.io/gh/ballerina-platform/module-ballerinax-kafka)

The Ballerina `kafka` module is one of the <a target="_blank" href="https://ballerina.io/"> Ballerina</a> language standard library modules.

This module helps to communicate with Kafka brokers as Producers and Consumers.

For example demonstrations of the usage, go to [Ballerina By Examples](https://ballerina.io/learn/by-example/).

## Issues and Projects 

Issues and Projects tabs are disabled for this repository as this is part of the Ballerina Standard Library. To report bugs, request new features, start new discussions, view project boards, etc., go to the [Ballerina Standard Library parent repository](https://github.com/ballerina-platform/ballerina-standard-library). 

This repository only contains the source code for the module.

## Building from the Source

### Setting Up the Prerequisites

* Download and install Java SE Development Kit (JDK) version 11 (from one of the following locations).

   * [Oracle](https://www.oracle.com/java/technologies/javase-jdk11-downloads.html)

   * [OpenJDK](https://adoptopenjdk.net/)

        > **Note:** Set the JAVA_HOME environment variable to the path name of the directory into which you installed JDK.

2. Download and install [Docker](https://www.docker.com/). This is required to run the tests.

### Building the Source

Execute the commands below to build from the source.

1. To build the library:
   ```    
   ./gradlew clean build
   ```

2. To run the integration tests:
   ```
   ./gradlew clean test
   ```
3. To build the module without the tests:
   ```
   ./gradlew clean build -x test
   ```
4. To debug package implementation:
   ```
   ./gradlew clean build -Pdebug=<port>
   ```
5. To debug the module with Ballerina language:
   ```
   ./gradlew clean build -PbalJavaDebug=<port>
   ```
6. Publish ZIP artifact to the local `.m2` repository:
   ```
   ./gradlew clean build publishToMavenLocal
   ```
7. Publish the generated artifacts to the local Ballerina central repository:
   ```
   ./gradlew clean build -PpublishToLocalCentral=true
   ```
8. Publish the generated artifacts to the Ballerina central repository:
   ```
   ./gradlew clean build -PpublishToCentral=true
   ```

## Contributing to Ballerina

As an open source project, Ballerina welcomes contributions from the community. 

For more information, go to the [contribution guidelines](https://github.com/ballerina-platform/ballerina-lang/blob/master/CONTRIBUTING.md).

## Code of Conduct

All the contributors are encouraged to read the [Ballerina Code of Conduct](https://ballerina.io/code-of-conduct).

## Useful Links

* Chat live with us via our [Slack channel](https://ballerina.io/community/slack/).
* Post all technical questions on Stack Overflow with the [#ballerina](https://stackoverflow.com/questions/tagged/ballerina) tag.
