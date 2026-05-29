## Overview

This module provides a DynamoDB-backed short-term memory store to use with AI messages (e.g., with AI agents, model providers, etc.).

### Key Features

- Amazon DynamoDB-backed persistent storage for short-term AI message memory
- One item per message using a composite primary key (partition key + sort key), so each session's history scales independently
- The system message is stored as a singleton item and overwritten in place; interactive messages are appended in insertion order via a per-key sequence counter
- Configurable maximum messages per key with automatic enforcement
- Built-in in-memory caching for improved read performance
- Automatic table creation on initialization, with a configurable billing mode
- Support for both connection configuration and an existing `dynamodb:Client`

## Prerequisites

- An AWS account with DynamoDB access and credentials (access key ID and secret access key). Follow [this guide](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_credentials_access-keys.html) to obtain credentials.
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

    Optionally, specify the maximum number of messages per key (`maxMessagesPerKey` - defaults to `20`), the in-memory cache configuration (`cacheConfig`), and the table-level configuration via `tableConfig` (a `dynamodb:TableConfig` record). `tableConfig` groups the DynamoDB-specific settings: the table name (`tableName` - defaults to `"chat_memory"`), the billing mode used when the connector creates the table (`billingMode` - defaults to `dynamodb:PAY_PER_REQUEST`, with `readCapacityUnits`/`writeCapacityUnits` applied only when `billingMode` is `dynamodb:PROVISIONED`), the read consistency model (`consistentReads`), and optional `tags` and `sseSpecification` applied at table creation.

    ```ballerina
    ai:ShortTermMemoryStore store = check new dynamodb:ShortTermMemoryStore({
        awsCredentials: {accessKeyId, secretAccessKey},
        region
    }, 10, {capacity: 10}, tableConfig = {tableName: "my_app_memory"});
    ```

## Storage model

The connector stores every message as its own item in a single DynamoDB table with a composite primary key:

| Attribute | Key role | Description |
|---|---|---|
| `MemoryKey` | Partition key (`HASH`) | The memory/session key. |
| `MessageId` | Sort key (`RANGE`) | `system` for the singleton system message, `counter` for the per-key sequence counter, and `msg#<zero-padded-sequence>` for interactive messages. |
| `Body` | Attribute | The JSON-encoded message body (for `system` and `msg#` items). |
| `Seq` | Attribute | The monotonically increasing interactive sequence value (for the `counter` item). |

On initialization, the connector checks whether the table exists (`DescribeTable`) and creates it (`CreateTable`) with the schema above if it does not, waiting until the table becomes `ACTIVE`.
