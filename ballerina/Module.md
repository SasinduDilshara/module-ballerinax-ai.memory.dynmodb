# Ballerina DynamoDB Short-Term Memory Store

## Overview

This Ballerina module provides an Amazon DynamoDB-backed short-term memory store for AI chat messages. It implements the `ai:ShortTermMemoryStore` interface, enabling AI agents and model providers to persist conversation history using DynamoDB as the storage backend.

## Features

- **DynamoDB-backed storage**: Persistent storage of chat messages, one item per message, in a single DynamoDB table with a composite primary key
- **Configurable message limits**: Set the maximum number of interactive messages per session key (default: 20)
- **In-memory caching**: Optional cache layer for improved read performance; disabled by default and only activated when the `cacheConfig` parameter is provided to the store initializer
- **Automatic table creation**: The table is created on initialization if it does not already exist, with a configurable billing mode
- **Flexible initialization**: Use either a connection configuration or a pre-created `dynamodb:Client`

## Prerequisites

- [Ballerina Swan Lake](https://ballerina.io/downloads/)
- An AWS account with DynamoDB access and credentials

## Getting Started

### Configuration-based Setup

```ballerina
import ballerinax/ai.memory.dynamodb;

dynamodb:ShortTermMemoryStore store = check new ({
    awsCredentials: {accessKeyId: "...", secretAccessKey: "..."},
    region: "us-east-1"
});
```

### Client-based Setup

```ballerina
import ballerinax/ai.memory.dynamodb;
import ballerinax/aws.dynamodb as dynamodbClient;

dynamodbClient:Client cl = check new ({
    awsCredentials: {accessKeyId: "...", secretAccessKey: "..."},
    region: "us-east-1"
});

dynamodb:ShortTermMemoryStore store = check new (cl);
```

## Customization

### Message Capacity

```ballerina
dynamodb:ShortTermMemoryStore store = check new ({
    awsCredentials: {accessKeyId: "...", secretAccessKey: "..."},
    region: "us-east-1"
}, maxMessagesPerKey = 50);
```

### Cache Configuration

```ballerina
import ballerina/cache;

dynamodb:ShortTermMemoryStore store = check new ({
    awsCredentials: {accessKeyId: "...", secretAccessKey: "..."},
    region: "us-east-1"
}, cacheConfig = {capacity: 30, evictionFactor: 0.2});
```

### Table Name and Billing Mode

```ballerina
import ballerinax/aws.dynamodb;

dynamodb:ShortTermMemoryStore store = check new ({
    awsCredentials: {accessKeyId: "...", secretAccessKey: "..."},
    region: "us-east-1"
}, tableConfig = {
    tableName: "my_app_memory",
    billingMode: dynamodb:PROVISIONED,
    readCapacityUnits: 10,
    writeCapacityUnits: 10
});
```

## License

This module is available under the [Apache 2.0 License](LICENSE).
