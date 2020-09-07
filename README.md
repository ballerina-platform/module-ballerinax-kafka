# Ballerina Kafka Module


[![Build Master Branch](https://github.com/ballerina-platform/module-ballerinax-kafka/workflows/Build/badge.svg)](https://github.com/ballerina-platform/module-ballerinax-kafka/actions?query=workflow%3ABuild%22)
[![Daily Build](https://github.com/ballerina-platform/module-ballerinax-kafka/workflows/Daily%20Build/badge.svg)](https://github.com/ballerina-platform/module-ballerinax-kafka/actions?query=workflow%3ADaily+Build%22)
[![GitHub Last Commit](https://img.shields.io/github/last-commit/ballerina-platform/module-ballerinax-kafka.svg)](https://github.com/ballerina-platform/module-ballerinax-kafka/commits/master)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

Ballerina Kafka module is one of the Ballerina language standard library modules.

This module helps to communicate with Kafka brokers as Producers and Consumers.

## Building from the Source

### Setting Up the Prerequisites

1. Download and install Java SE Development Kit (JDK) version 8 (from one of the following locations).

   * [Oracle](https://www.oracle.com/java/technologies/javase/javase-jdk8-downloads.html)
   
   * [OpenJDK](http://openjdk.java.net/install/index.html)
   
        > **Note:** Set the JAVA_HOME environment variable to the path name of the directory into which you installed JDK.

2. Download and install [Docker](https://www.docker.com/). This is required to run the tests.

### Building the Source

Execute the commands below to build from the source.

1. To build the library:
```shell script
./gradlew clean build
```

2. To build the module without the tests:
```shell script
./gradlew clean build -PskipBallerinaTests
```

4. To debug the tests:
```shell script
./gradlew clean test -PdebugBallerina=<port>
```

## Contributing to Ballerina

As an open source project, Ballerina welcomes contributions from the community. 

You can also check for [open issues](https://github.com/ballerina-platform/module-ballerinax-kafka/issues) that interest you. We look forward to receiving your contributions.

For more information, go to the [contribution guidelines](https://github.com/ballerina-platform/ballerina-lang/blob/master/CONTRIBUTING.md).

## Code of Conduct

All the contributors are encouraged to read the [Ballerina Code of Conduct](https://ballerina.io/code-of-conduct).

## Useful Links

* Discuss the code changes of the Ballerina project in [ballerina-dev@googlegroups.com](mailto:ballerina-dev@googlegroups.com).
* Chat live with us via our [Slack channel](https://ballerina.io/community/slack/).
* Post all technical questions on Stack Overflow with the [#ballerina](https://stackoverflow.com/questions/tagged/ballerina) tag.
