# Ballerina DynamoDB-backed short-term chat message store connector

[![Build](https://github.com/ballerina-platform/module-ballerinax-ai.memory.dynamodb/actions/workflows/ci.yml/badge.svg)](https://github.com/ballerina-platform/module-ballerinax-ai.memory.dynamodb/actions/workflows/ci.yml)
[![GitHub Last Commit](https://img.shields.io/github/last-commit/ballerina-platform/module-ballerinax-ai.memory.dynamodb.svg)](https://github.com/ballerina-platform/module-ballerinax-ai.memory.dynamodb/commits/main)
[![GitHub Issues](https://img.shields.io/github/issues/ballerina-platform/ballerina-library/module/ai.memory.dynamodb.svg?label=Open%20Issues)](https://github.com/ballerina-platform/ballerina-library/labels/module%2Fai.memory.dynamodb)

## Overview

This module provides an Amazon DynamoDB-backed short-term memory store to use with AI messages (e.g., with AI agents, model providers, etc.).

## Prerequisites

- An AWS account with DynamoDB access and credentials (access key ID and secret access key).
- The credentials must allow the `DescribeTable`, `CreateTable`, `GetItem`, `PutItem`, `UpdateItem`, `DeleteItem`, `Query`, and `BatchWriteItem` actions.

## Quickstart

Follow the steps below to use this store in your Ballerina application:

1. Import the `ballerinax/ai.memory.dynamodb` module.

```ballerina
import ballerinax/ai.memory.dynamodb;
```

Optionally, import the `ballerina/ai` and/or `ballerinax/aws.dynamodb` module(s).

```ballerina
import ballerina/ai;
import ballerinax/aws.dynamodb;
```

2. Create the short-term memory store by passing either the connection configuration or a `dynamodb:Client`.

    i. Using the connection configuration

    ```ballerina
    import ballerina/ai;
    import ballerinax/ai.memory.dynamodb;

    configurable string accessKeyId = ?;
    configurable string secretAccessKey = ?;
    configurable string region = ?;

    ai:ShortTermMemoryStore store = check new dynamodb:ShortTermMemoryStore({
        awsCredentials: {accessKeyId, secretAccessKey},
        region
    });
    ```

    ii. Using an existing `dynamodb:Client`

    ```ballerina
    import ballerina/ai;
    import ballerinax/aws.dynamodb;
    import ballerinax/ai.memory.dynamodb as dynamodbStore;

    configurable string accessKeyId = ?;
    configurable string secretAccessKey = ?;
    configurable string region = ?;

    dynamodb:Client dynamodbClient = check new ({
        awsCredentials: {accessKeyId, secretAccessKey},
        region
    });
    ai:ShortTermMemoryStore store = check new dynamodbStore:ShortTermMemoryStore(dynamodbClient);
    ```

    Optionally, specify the maximum number of messages to store per key (`maxMessagesPerKey` - defaults to `20`), the configuration for the in-memory cache (`cacheConfig`), and the table-level configuration via `tableConfig` (a `dynamodb:TableConfig` record). `tableConfig` groups the DynamoDB-specific settings: the table name (`tableName` - defaults to `"chat_memory"`), the billing mode used when the connector creates the table (`billingMode` - defaults to `dynamodb:PAY_PER_REQUEST`), the read consistency model (`consistentReads`), and optional `tags` and `sseSpecification` applied at table creation.

    ```ballerina
    ai:ShortTermMemoryStore store = check new dynamodb:ShortTermMemoryStore({
        awsCredentials: {accessKeyId, secretAccessKey},
        region
    }, 10, {capacity: 10}, tableConfig = {tableName: "my_app_memory"});
    ```

## Examples

The `ai.memory.dynamodb` connector provides a practical example illustrating usage in a real-world scenario.

1. [Chat memory with an agent](examples/chat-memory-with-agent) - Wire the DynamoDB-backed store into an `ai:Agent` and run a multi-turn conversation that persists across turns.

## Build from the source

### Setting up the prerequisites

1. Download and install Java SE Development Kit (JDK) version 21. You can download it from either of the following sources:

    * [Oracle JDK](https://www.oracle.com/java/technologies/downloads/)
    * [OpenJDK](https://adoptium.net/)

   > **Note:** After installation, remember to set the `JAVA_HOME` environment variable to the directory where JDK was installed.

2. Download and install [Ballerina Swan Lake](https://ballerina.io/).

3. Download and install [Docker](https://www.docker.com/get-started).

   > **Note**: Ensure that the Docker daemon is running before executing any tests.

4. Export a GitHub personal access token with `read:packages` permission as follows:

    ```bash
    export packageUser=<Username>
    export packagePAT=<Personal access token>
    ```

### Build options

Execute the commands below to build from the source.

1. To build the package:

   ```bash
   ./gradlew clean build
   ```

2. To run the tests:

   ```bash
   ./gradlew clean test
   ```

3. To build without the tests:

   ```bash
   ./gradlew clean build -x test
   ```

4. To debug the package with a remote debugger:

   ```bash
   ./gradlew clean build -Pdebug=<port>
   ```

5. To debug with the Ballerina language:

   ```bash
   ./gradlew clean build -PbalJavaDebug=<port>
   ```

6. To publish the generated artifacts to the local Ballerina Central repository:

    ```bash
    ./gradlew clean build -PpublishToLocalCentral=true
    ```

7. To publish the generated artifacts to the Ballerina Central repository:

   ```bash
   ./gradlew clean build -PpublishToCentral=true
   ```

> **Note:** The `ballerinax/aws.dynamodb` client builds its endpoint from the AWS region and cannot target a local DynamoDB container. The integration tests therefore require real AWS credentials, supplied via `ballerina/tests/Config.toml` (`accessKeyId`, `secretAccessKey`, `region`).

## Contribute to Ballerina

As an open-source project, Ballerina welcomes contributions from the community.

For more information, go to the [contribution guidelines](https://github.com/ballerina-platform/ballerina-lang/blob/master/CONTRIBUTING.md).

## Code of conduct

All the contributors are encouraged to read the [Ballerina Code of Conduct](https://ballerina.io/code-of-conduct).

## Useful links

* For more information go to the [`ai.memory.dynamodb` package](https://central.ballerina.io/ballerinax/ai.memory.dynamodb/latest).
* For example demonstrations of the usage, go to [Ballerina By Examples](https://ballerina.io/learn/by-example/).
* Chat live with us via our [Discord server](https://discord.gg/ballerinalang).
* Post all technical questions on Stack Overflow with the [#ballerina](https://stackoverflow.com/questions/tagged/ballerina) tag.
