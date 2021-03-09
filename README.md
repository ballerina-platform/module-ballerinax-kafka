# Ballerina Kafka Module


[![Build](https://github.com/ballerina-platform/module-ballerinax-kafka/workflows/Build/badge.svg)](https://github.com/ballerina-platform/module-ballerinax-kafka/actions?query=workflow%3ABuild)
[![GitHub Last Commit](https://img.shields.io/github/last-commit/ballerina-platform/module-ballerinax-kafka.svg)](https://github.com/ballerina-platform/module-ballerinax-kafka/commits/master)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![codecov](https://codecov.io/gh/ballerina-platform/module-ballerinax-kafka/branch/master/graph/badge.svg)](https://codecov.io/gh/ballerina-platform/module-ballerinax-kafka)

Ballerina Kafka module is one of the Ballerina language standard library modules.

This module helps to communicate with Kafka brokers as Producers and Consumers.

## Issues and Projects 

Issues and Projects tabs are disabled for this repository as this is part of the Ballerina Standard Library. To report bugs, request new features, start new discussions, view project boards, etc. please visit Ballerina Standard Library [parent repository](https://github.com/ballerina-platform/ballerina-standard-library). 

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
```shell script
  ./gradlew clean build
```

2. To debug the tests
```shell script
  ./gradlew clean build -Pdebug=<port>
```

3. To build the module without the tests:
```shell script
  ./gradlew clean build -x test
```

## Contributing to Ballerina

As an open source project, Ballerina welcomes contributions from the community. 

For more information, go to the [contribution guidelines](https://github.com/ballerina-platform/ballerina-lang/blob/master/CONTRIBUTING.md).

## Code of Conduct

All the contributors are encouraged to read the [Ballerina Code of Conduct](https://ballerina.io/code-of-conduct).

## Useful Links

* Discuss the code changes of the Ballerina project in [ballerina-dev@googlegroups.com](mailto:ballerina-dev@googlegroups.com).
* Chat live with us via our [Slack channel](https://ballerina.io/community/slack/).
* Post all technical questions on Stack Overflow with the [#ballerina](https://stackoverflow.com/questions/tagged/ballerina) tag.
