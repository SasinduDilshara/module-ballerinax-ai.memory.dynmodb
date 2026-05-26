// Copyright (c) 2025, WSO2 LLC. (http://www.wso2.com).
//
// WSO2 LLC. licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except
// in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

import ballerina/ai;
import ballerina/cache;
import ballerina/test;
import ballerinax/aws.dynamodb;

const string K1 = "key1";
const string K2 = "key2";
const string K3 = "key3";

const string TABLE_NAME = "chat_memory";

// The integration tests require real AWS credentials. The `ballerinax/aws.dynamodb`
// client builds its endpoint from the region only and cannot target DynamoDB Local.
// Supply these via `tests/Config.toml` when running against a live AWS account.
configurable string accessKeyId = "test";
configurable string secretAccessKey = "test";
configurable string region = "us-east-1";

const ai:ChatSystemMessage K1SM1 = {role: ai:SYSTEM, content: "You are a helpful assistant that is aware of the weather."};

const ai:ChatUserMessage K1M1 = {role: ai:USER, content: "Hello, my name is Alice. I'm from Seattle."};
final readonly & ai:ChatAssistantMessage k1m2 = {role: ai:ASSISTANT, content: "Hello Alice, what can I do for you?"};
const ai:ChatUserMessage K1M3 = {role: ai:USER, content: "I would like to know the weather today."};
final readonly & ai:ChatAssistantMessage K1M4 = {
    role: ai:ASSISTANT,
    content: "The weather in Seattle today is mostly cloudy with occasional showers and a high around 58°F."
};

const ai:ChatUserMessage K2M1 = {role: ai:USER, content: "Hello, my name is Bob."};

isolated dynamodb:Client? modCl = ();

@test:BeforeSuite
function initClient() returns error? {
    dynamodb:Client cl = check new ({
        awsCredentials: {accessKeyId, secretAccessKey},
        region
    });
    lock {
        modCl = cl;
    }
    // Initialize a store once so the backing table is created before the tests run.
    _ = check new ShortTermMemoryStore(cl);
}

function getClient() returns dynamodb:Client {
    lock {
        return <dynamodb:Client>modCl;
    }
}

// Drains a query result stream into an array of items.
function queryItems(dynamodb:Client cl, dynamodb:QueryInput queryInput)
        returns map<dynamodb:AttributeValue>[]|error {
    stream<dynamodb:QueryOutput, error?> resultStream = check cl->query(queryInput);
    map<dynamodb:AttributeValue>[] items = [];
    while true {
        record {|dynamodb:QueryOutput value;|}? next = check resultStream.next();
        if next is () {
            break;
        }
        map<dynamodb:AttributeValue>? item = next.value?.Item;
        if item is map<dynamodb:AttributeValue> {
            items.push(item);
        }
    }
    return items;
}

function cleanupKeys() returns error? {
    dynamodb:Client cl = getClient();
    foreach string key in [K1, K2, K3] {
        check deleteAllForKey(cl, TABLE_NAME, key);
    }
}

function deleteAllForKey(dynamodb:Client cl, string tableName, string key) returns error? {
    map<dynamodb:AttributeValue>[] items = check queryItems(cl, {
        TableName: tableName,
        ConsistentRead: true,
        ProjectionExpression: "#sk",
        KeyConditionExpression: "#pk = :pk",
        ExpressionAttributeNames: {"#pk": PARTITION_KEY_ATTRIBUTE, "#sk": SORT_KEY_ATTRIBUTE},
        ExpressionAttributeValues: {":pk": {S: key}}
    });
    foreach map<dynamodb:AttributeValue> item in items {
        dynamodb:AttributeValue? sortKeyAttr = item[SORT_KEY_ATTRIBUTE];
        string? sortId = sortKeyAttr is () ? () : sortKeyAttr?.S;
        if sortId is string {
            _ = check cl->deleteItem({TableName: tableName, Key: itemKey(key, sortId)});
        }
    }
}

@test:Config {
    before: cleanupKeys
}
function testBasicStore() returns error? {
    dynamodb:Client cl = getClient();
    ShortTermMemoryStore store = check new (cl);

    check store.put(K1, K1SM1);
    check store.put(K1, K1M1);
    check store.put(K1, k1m2);
    check store.put(K2, K2M1);

    check assertFromDynamoDb(cl, K1, [K1SM1], SYSTEM);
    check assertFromDynamoDb(cl, K1, [K1M1, k1m2], INTERACTIVE);
    check assertFromDynamoDb(cl, K1, [K1SM1, K1M1, k1m2]);

    check assertAllMessages(store, K1, [K1SM1, K1M1, k1m2]);
    check assertSystemMessage(store, K1, K1SM1);
    check assertInteractiveMessages(store, K1, [K1M1, k1m2]);

    check assertFromDynamoDb(cl, K2, [], SYSTEM);
    check assertFromDynamoDb(cl, K2, [K2M1], INTERACTIVE);
    check assertFromDynamoDb(cl, K2, [K2M1]);

    check assertAllMessages(store, K2, [K2M1]);
    check assertSystemMessage(store, K2, ());
    check assertInteractiveMessages(store, K2, [K2M1]);

    check store.removeAll(K1);

    check assertFromDynamoDb(cl, K1, [], SYSTEM);
    check assertFromDynamoDb(cl, K1, [], INTERACTIVE);
    check assertFromDynamoDb(cl, K1, []);

    check assertAllMessages(store, K1, []);
    check assertSystemMessage(store, K1, ());
    check assertInteractiveMessages(store, K1, []);

    check assertFromDynamoDb(cl, K2, [], SYSTEM);
    check assertFromDynamoDb(cl, K2, [K2M1], INTERACTIVE);
    check assertFromDynamoDb(cl, K2, [K2M1]);

    check assertAllMessages(store, K2, [K2M1]);
    check assertSystemMessage(store, K2, ());
    check assertInteractiveMessages(store, K2, [K2M1]);

    // Add more messages to K1 after deletion.
    check store.put(K1, K1M3);

    check assertFromDynamoDb(cl, K1, [], SYSTEM);
    check assertFromDynamoDb(cl, K1, [K1M3], INTERACTIVE);
    check assertFromDynamoDb(cl, K1, [K1M3]);

    check assertAllMessages(store, K1, [K1M3]);
    check assertSystemMessage(store, K1, ());
    check assertInteractiveMessages(store, K1, [K1M3]);
}

@test:Config {
    before: cleanupKeys
}
function testRemoveSystemMessage() returns error? {
    dynamodb:Client cl = getClient();
    ShortTermMemoryStore store = check new (cl);

    check store.put(K1, K1SM1);
    check store.put(K1, K1M1);
    check store.put(K1, k1m2);
    check store.put(K2, K2M1);

    check store.removeChatSystemMessage(K1);

    check assertFromDynamoDb(cl, K1, [], SYSTEM);
    check assertFromDynamoDb(cl, K1, [K1M1, k1m2], INTERACTIVE);
    check assertFromDynamoDb(cl, K1, [K1M1, k1m2]);

    check assertAllMessages(store, K1, [K1M1, k1m2]);
    check assertSystemMessage(store, K1, ());
    check assertInteractiveMessages(store, K1, [K1M1, k1m2]);

    check assertFromDynamoDb(cl, K2, [], SYSTEM);
    check assertFromDynamoDb(cl, K2, [K2M1], INTERACTIVE);
    check assertFromDynamoDb(cl, K2, [K2M1]);

    check assertAllMessages(store, K2, [K2M1]);
    check assertSystemMessage(store, K2, ());
    check assertInteractiveMessages(store, K2, [K2M1]);

    check store.removeChatSystemMessage(K2);

    check assertFromDynamoDb(cl, K2, [], SYSTEM);
    check assertFromDynamoDb(cl, K2, [K2M1], INTERACTIVE);
    check assertFromDynamoDb(cl, K2, [K2M1]);

    check assertAllMessages(store, K2, [K2M1]);
    check assertSystemMessage(store, K2, ());
    check assertInteractiveMessages(store, K2, [K2M1]);
}

@test:Config {
    before: cleanupKeys
}
function testRemoveInteractiveMessages() returns error? {
    dynamodb:Client cl = getClient();
    ShortTermMemoryStore store = check new (cl);

    check store.put(K1, K1SM1);
    check store.put(K1, K1M1);
    check store.put(K1, k1m2);
    check store.put(K2, K2M1);

    check store.removeChatInteractiveMessages(K1);

    check assertFromDynamoDb(cl, K1, [K1SM1], SYSTEM);
    check assertFromDynamoDb(cl, K1, [], INTERACTIVE);
    check assertFromDynamoDb(cl, K1, [K1SM1]);

    check assertAllMessages(store, K1, [K1SM1]);
    check assertSystemMessage(store, K1, K1SM1);
    check assertInteractiveMessages(store, K1, []);

    check assertFromDynamoDb(cl, K2, [], SYSTEM);
    check assertFromDynamoDb(cl, K2, [K2M1], INTERACTIVE);
    check assertFromDynamoDb(cl, K2, [K2M1]);

    check assertAllMessages(store, K2, [K2M1]);
    check assertSystemMessage(store, K2, ());
    check assertInteractiveMessages(store, K2, [K2M1]);

    check store.removeChatInteractiveMessages(K2);

    check assertFromDynamoDb(cl, K1, [K1SM1], SYSTEM);
    check assertFromDynamoDb(cl, K1, [], INTERACTIVE);
    check assertFromDynamoDb(cl, K1, [K1SM1]);

    check assertAllMessages(store, K1, [K1SM1]);
    check assertSystemMessage(store, K1, K1SM1);
    check assertInteractiveMessages(store, K1, []);

    check assertFromDynamoDb(cl, K2, [], SYSTEM);
    check assertFromDynamoDb(cl, K2, [], INTERACTIVE);
    check assertFromDynamoDb(cl, K2, []);

    check assertAllMessages(store, K2, []);
    check assertSystemMessage(store, K2, ());
    check assertInteractiveMessages(store, K2, []);
}

@test:Config {
    before: cleanupKeys
}
function testRemoveAllMessages() returns error? {
    dynamodb:Client cl = getClient();
    ShortTermMemoryStore store = check new (cl);

    check store.put(K1, K1SM1);
    check store.put(K1, K1M1);
    check store.put(K1, k1m2);
    check store.put(K2, K2M1);

    check store.removeAll(K1);

    check assertFromDynamoDb(cl, K1, [], SYSTEM);
    check assertFromDynamoDb(cl, K1, [], INTERACTIVE);
    check assertFromDynamoDb(cl, K1, []);

    check assertAllMessages(store, K1, []);
    check assertSystemMessage(store, K1, ());
    check assertInteractiveMessages(store, K1, []);

    check assertFromDynamoDb(cl, K2, [], SYSTEM);
    check assertFromDynamoDb(cl, K2, [K2M1], INTERACTIVE);
    check assertFromDynamoDb(cl, K2, [K2M1]);

    check assertAllMessages(store, K2, [K2M1]);
    check assertSystemMessage(store, K2, ());
    check assertInteractiveMessages(store, K2, [K2M1]);

    check store.removeAll(K2);

    check assertFromDynamoDb(cl, K1, [], SYSTEM);
    check assertFromDynamoDb(cl, K1, [], INTERACTIVE);
    check assertFromDynamoDb(cl, K1, []);

    check assertAllMessages(store, K1, []);
    check assertSystemMessage(store, K1, ());
    check assertInteractiveMessages(store, K1, []);

    check assertFromDynamoDb(cl, K2, [], SYSTEM);
    check assertFromDynamoDb(cl, K2, [], INTERACTIVE);
    check assertFromDynamoDb(cl, K2, []);

    check assertAllMessages(store, K2, []);
    check assertSystemMessage(store, K2, ());
    check assertInteractiveMessages(store, K2, []);
}

@test:Config {
    before: cleanupKeys
}
function testRemovingSubsetOfInteractiveMessages() returns error? {
    dynamodb:Client cl = getClient();
    ShortTermMemoryStore store = check new (cl);

    check store.put(K1, K1SM1);
    check store.put(K1, K1M1);
    check store.put(K1, k1m2);
    check store.put(K1, K1M3);
    check store.put(K1, K1M4);

    check store.removeChatInteractiveMessages(K1, 2);

    check assertFromDynamoDb(cl, K1, [K1SM1], SYSTEM);
    check assertFromDynamoDb(cl, K1, [K1M3, K1M4], INTERACTIVE);
    check assertFromDynamoDb(cl, K1, [K1SM1, K1M3, K1M4]);

    check assertSystemMessage(store, K1, K1SM1);
    check assertInteractiveMessages(store, K1, [K1M3, K1M4]);
    check assertAllMessages(store, K1, [K1SM1, K1M3, K1M4]);
}

@test:Config {
    before: cleanupKeys
}
function testSystemMessageOverwrite() returns error? {
    dynamodb:Client cl = getClient();
    ShortTermMemoryStore store = check new (cl);

    check store.put(K1, K1SM1);
    check store.put(K1, K1M1);
    check store.put(K1, k1m2);

    check assertSystemMessage(store, K1, K1SM1);
    check assertInteractiveMessages(store, K1, [K1M1, k1m2]);
    check assertAllMessages(store, K1, [K1SM1, K1M1, k1m2]);

    check assertFromDynamoDb(cl, K1, [K1SM1], SYSTEM);
    check assertFromDynamoDb(cl, K1, [K1M1, k1m2], INTERACTIVE);
    check assertFromDynamoDb(cl, K1, [K1SM1, K1M1, k1m2]);

    final readonly & ai:ChatSystemMessage k1sm2 = {
        role: ai:SYSTEM,
        content: "You are a helpful assistant that is aware of sports."
    };
    check store.put(K1, k1sm2);

    check assertSystemMessage(store, K1, k1sm2);
    check assertInteractiveMessages(store, K1, [K1M1, k1m2]);
    check assertAllMessages(store, K1, [k1sm2, K1M1, k1m2]);

    check assertFromDynamoDb(cl, K1, [k1sm2], SYSTEM);
    check assertFromDynamoDb(cl, K1, [K1M1, k1m2], INTERACTIVE);
    check assertFromDynamoDb(cl, K1, [k1sm2, K1M1, k1m2]);

    // Verify only one system message exists in DynamoDB (PutItem overwrites).
    string systemBody = check readSystemBody(cl, TABLE_NAME, K1);
    ChatSystemMessageDatabaseMessage dbSystemMessage = check systemBody.fromJsonStringWithType();
    assertChatMessageEquals(transformFromSystemMessageDatabaseMessage(dbSystemMessage), k1sm2);
}

@test:Config {
    before: cleanupKeys
}
function testSystemMessageOverwriteWithPutAll() returns error? {
    dynamodb:Client cl = getClient();
    ShortTermMemoryStore store = check new (cl);

    final readonly & ai:ChatSystemMessage k1sm2 = {
        role: ai:SYSTEM,
        content: "You are a helpful assistant that is aware of sports."
    };
    check store.put(K1, [K1SM1, K1M1, k1m2, k1sm2]);
    check assertSystemMessage(store, K1, k1sm2);
    check assertFromDynamoDb(cl, K1, [k1sm2, K1M1, k1m2]);

    // Verify only one system message value in DynamoDB.
    string systemBody = check readSystemBody(cl, TABLE_NAME, K1);
    ChatSystemMessageDatabaseMessage dbSystemMessage = check systemBody.fromJsonStringWithType();
    assertChatMessageEquals(transformFromSystemMessageDatabaseMessage(dbSystemMessage), k1sm2);
}

@test:Config {
    before: cleanupKeys
}
function testPutWithDifferentMessageKinds() returns error? {
    dynamodb:Client cl = getClient();
    ShortTermMemoryStore store = check new (cl);

    final readonly & ai:ChatFunctionMessage funcMessage = {
        role: "function",
        name: "getWeather",
        id: "func1"
    };

    check store.put(K1, K1SM1);
    check store.put(K1, K1M1);
    check store.put(K1, k1m2);
    check store.put(K1, funcMessage);

    check assertFromDynamoDb(cl, K1, [K1SM1], SYSTEM);
    check assertFromDynamoDb(cl, K1, [K1M1, k1m2, funcMessage], INTERACTIVE);
    check assertFromDynamoDb(cl, K1, [K1SM1, K1M1, k1m2, funcMessage]);

    check assertSystemMessage(store, K1, K1SM1);
    check assertInteractiveMessages(store, K1, [K1M1, k1m2, funcMessage]);
    check assertAllMessages(store, K1, [K1SM1, K1M1, k1m2, funcMessage]);
}

@test:Config {
    before: cleanupKeys
}
function testUpdateWithSystemMessageWhenInteractiveMessagesPresentOnStart() returns error? {
    dynamodb:Client cl = getClient();

    // Pre-populate DynamoDB with interactive messages using a separate store instance,
    // simulating data that existed before this store object was created.
    ShortTermMemoryStore seedStore = check new (cl, 5);
    check seedStore.put(K1, K1M1);
    check seedStore.put(K1, k1m2);

    ShortTermMemoryStore store = check new (cl, 5);
    check store.put(K1, K1SM1);

    check assertFromDynamoDb(cl, K1, [K1SM1], SYSTEM);
    check assertFromDynamoDb(cl, K1, [K1M1, k1m2], INTERACTIVE);
    check assertFromDynamoDb(cl, K1, [K1SM1, K1M1, k1m2]);

    check assertSystemMessage(store, K1, K1SM1);
    check assertInteractiveMessages(store, K1, [K1M1, k1m2]);
    check assertAllMessages(store, K1, [K1SM1, K1M1, k1m2]);
}

function assertAllMessages(ShortTermMemoryStore store, string key, ai:ChatMessage[] expected) returns error? {
    ai:ChatMessage[] actual = check store.getAll(key);
    int actualLength = actual.length();
    test:assertEquals(actualLength, expected.length());
    foreach var index in 0 ..< actualLength {
        assertChatMessageEquals(actual[index], expected[index]);
    }
}

function assertSystemMessage(ShortTermMemoryStore store, string key, ai:ChatSystemMessage? expected) returns error? {
    ai:ChatSystemMessage? actual = check store.getChatSystemMessage(key);
    if expected is () && actual is () {
        return;
    }

    if expected is () || actual is () {
        test:assertFail("Actual and expected ChatSystemMessage do not match");
    }

    assertChatMessageEquals(actual, expected);
}

function assertInteractiveMessages(ShortTermMemoryStore store, string key, ai:ChatInteractiveMessage[] expected) returns error? {
    ai:ChatInteractiveMessage[] actual = check store.getChatInteractiveMessages(key);
    int actualLength = actual.length();
    test:assertEquals(actualLength, expected.length());
    foreach var index in 0 ..< actualLength {
        assertChatMessageEquals(actual[index], expected[index]);
    }
}

enum MessageType {
    SYSTEM,
    INTERACTIVE,
    ALL
}

// Reads the raw JSON body of the system message item directly from DynamoDB.
function readSystemBody(dynamodb:Client cl, string tableName, string key) returns string|error {
    dynamodb:ItemGetOutput result = check cl->getItem({
        TableName: tableName,
        Key: itemKey(key, SYSTEM_MESSAGE_ID),
        ConsistentRead: true
    });
    map<dynamodb:AttributeValue>? item = result?.Item;
    if item is () {
        return error("Expected a system message item but found none.");
    }
    return extractBody(item);
}

function assertFromDynamoDb(dynamodb:Client cl, string key, ai:ChatMessage[] expected,
        MessageType messageType = ALL) returns error? {
    ai:ChatMessage[] actualMessages = [];

    if messageType == SYSTEM || messageType == ALL {
        dynamodb:ItemGetOutput sysResult = check cl->getItem({
            TableName: TABLE_NAME,
            Key: itemKey(key, SYSTEM_MESSAGE_ID),
            ConsistentRead: true
        });
        map<dynamodb:AttributeValue>? sysItem = sysResult?.Item;
        if sysItem is map<dynamodb:AttributeValue> {
            string body = check extractBody(sysItem);
            ChatSystemMessageDatabaseMessage|error dbMsg = body.fromJsonStringWithType();
            if dbMsg is error {
                test:assertFail("Failed to parse system message from DynamoDB: " + dbMsg.message());
            }
            actualMessages.push(transformFromDatabaseMessage(dbMsg));
        }
    }

    if messageType == INTERACTIVE || messageType == ALL {
        map<dynamodb:AttributeValue>[] items = check queryItems(cl, {
            TableName: TABLE_NAME,
            ConsistentRead: true,
            ScanIndexForward: true,
            KeyConditionExpression: "#pk = :pk and begins_with(#sk, :prefix)",
            ExpressionAttributeNames: {"#pk": PARTITION_KEY_ATTRIBUTE, "#sk": SORT_KEY_ATTRIBUTE},
            ExpressionAttributeValues: {":pk": {S: key}, ":prefix": {S: INTERACTIVE_ID_PREFIX}}
        });
        foreach map<dynamodb:AttributeValue> item in items {
            string body = check extractBody(item);
            ChatInteractiveMessageDatabaseMessage|error dbMsg = body.fromJsonStringWithType();
            if dbMsg is error {
                test:assertFail("Failed to parse interactive message from DynamoDB: " + dbMsg.message());
            }
            actualMessages.push(transformFromInteractiveMessageDatabaseMessage(dbMsg));
        }
    }

    int actualLength = actualMessages.length();
    test:assertEquals(actualLength, expected.length());
    foreach var index in 0 ..< actualLength {
        assertChatMessageEquals(actualMessages[index], expected[index]);
    }
}

isolated function assertChatMessageEquals(ai:ChatMessage actual, ai:ChatMessage expected) {
    if (actual is ai:ChatUserMessage && expected is ai:ChatUserMessage) ||
            (actual is ai:ChatSystemMessage && expected is ai:ChatSystemMessage) {
        test:assertEquals(actual.role, expected.role);
        assertContentEquals(actual.content, expected.content);
        test:assertEquals(actual.name, expected.name);
        return;
    }

    if actual is ai:ChatFunctionMessage && expected is ai:ChatFunctionMessage {
        test:assertEquals(actual.role, expected.role);
        test:assertEquals(actual.name, expected.name);
        test:assertEquals(actual.id, expected.id);
        return;
    }

    if actual is ai:ChatAssistantMessage && expected is ai:ChatAssistantMessage {
        test:assertEquals(actual.role, expected.role);
        test:assertEquals(actual.content, expected.content);
        test:assertEquals(actual.name, expected.name);
        test:assertEquals(actual.toolCalls, expected.toolCalls);
        return;
    }

    test:assertFail("Actual and expected ChatMessage types do not match");
}

isolated function assertContentEquals(ai:Prompt|string actual, ai:Prompt|string expected) {
    if actual is string && expected is string {
        test:assertEquals(actual, expected);
        return;
    }

    if actual is ai:Prompt && expected is ai:Prompt {
        test:assertEquals(actual.strings, expected.strings);
        test:assertEquals(actual.insertions, expected.insertions);
        return;
    }

    test:assertFail("Actual and expected content do not match");
}

// Cache tests

@test:Config {
    before: cleanupKeys
}
function testBasicStoreWithCache() returns error? {
    dynamodb:Client cl = getClient();
    cache:CacheConfig cacheConfig = {
        capacity: 10,
        evictionFactor: 0.2
    };
    ShortTermMemoryStore store = check new (cl, cacheConfig = cacheConfig);

    check store.put(K1, K1SM1);
    check store.put(K1, K1M1);
    check store.put(K1, k1m2);
    check store.put(K2, K2M1);

    // First retrieval - should load from DynamoDB and cache.
    check assertAllMessages(store, K1, [K1SM1, K1M1, k1m2]);
    check assertInteractiveMessages(store, K1, [K1M1, k1m2]);

    // Second retrieval - should use cache.
    check assertAllMessages(store, K1, [K1SM1, K1M1, k1m2]);
    check assertInteractiveMessages(store, K1, [K1M1, k1m2]);

    check assertAllMessages(store, K2, [K2M1]);
    check assertInteractiveMessages(store, K2, [K2M1]);
}

@test:Config {
    before: cleanupKeys
}
function testBasicStoreWithCacheWithPutAll() returns error? {
    dynamodb:Client cl = getClient();
    cache:CacheConfig cacheConfig = {
        capacity: 10,
        evictionFactor: 0.2
    };
    ShortTermMemoryStore store = check new (cl, cacheConfig = cacheConfig);

    check store.put(K1, [K1SM1, K1M1, k1m2]);
    check store.put(K2, K2M1);

    check assertAllMessages(store, K1, [K1SM1, K1M1, k1m2]);
    check assertInteractiveMessages(store, K1, [K1M1, k1m2]);

    check assertAllMessages(store, K1, [K1SM1, K1M1, k1m2]);
    check assertInteractiveMessages(store, K1, [K1M1, k1m2]);

    check assertAllMessages(store, K2, [K2M1]);
    check assertInteractiveMessages(store, K2, [K2M1]);
}

@test:Config {
    before: cleanupKeys
}
function testCacheUpdateOnPut() returns error? {
    dynamodb:Client cl = getClient();
    cache:CacheConfig cacheConfig = {
        capacity: 10,
        evictionFactor: 0.2
    };
    ShortTermMemoryStore store = check new (cl, cacheConfig = cacheConfig);

    check store.put(K1, K1SM1);
    check store.put(K1, K1M1);

    // Load into cache.
    check assertAllMessages(store, K1, [K1SM1, K1M1]);

    // Add more messages - cache should be updated.
    check store.put(K1, k1m2);
    check store.put(K1, K1M3);

    // Verify cache reflects the updates.
    check assertAllMessages(store, K1, [K1SM1, K1M1, k1m2, K1M3]);
    check assertInteractiveMessages(store, K1, [K1M1, k1m2, K1M3]);
}

@test:Config {
    before: cleanupKeys
}
function testCacheUpdateWithPutAll() returns error? {
    dynamodb:Client cl = getClient();
    cache:CacheConfig cacheConfig = {
        capacity: 10,
        evictionFactor: 0.2
    };
    ShortTermMemoryStore store = check new (cl, cacheConfig = cacheConfig);

    check store.put(K1, [K1SM1, K1M1]);
    check assertAllMessages(store, K1, [K1SM1, K1M1]);

    check store.put(K1, [k1m2, K1M3]);

    check assertAllMessages(store, K1, [K1SM1, K1M1, k1m2, K1M3]);
    check assertInteractiveMessages(store, K1, [K1M1, k1m2, K1M3]);
}

@test:Config {
    before: cleanupKeys
}
function testCacheSystemMessageUpdate() returns error? {
    dynamodb:Client cl = getClient();
    cache:CacheConfig cacheConfig = {
        capacity: 10,
        evictionFactor: 0.2
    };
    ShortTermMemoryStore store = check new (cl, cacheConfig = cacheConfig);

    check store.put(K1, K1SM1);
    check store.put(K1, K1M1);

    check assertSystemMessage(store, K1, K1SM1);
    check assertAllMessages(store, K1, [K1SM1, K1M1]);

    final readonly & ai:ChatSystemMessage k1sm2 = {
        role: ai:SYSTEM,
        content: "You are a helpful assistant that is aware of sports."
    };
    check store.put(K1, k1sm2);

    check assertSystemMessage(store, K1, k1sm2);
    check assertAllMessages(store, K1, [k1sm2, K1M1]);
}

@test:Config {
    before: cleanupKeys
}
function testCacheSystemMessageUpdateOnPutAll() returns error? {
    dynamodb:Client cl = getClient();
    cache:CacheConfig cacheConfig = {
        capacity: 10,
        evictionFactor: 0.2
    };
    ShortTermMemoryStore store = check new (cl, cacheConfig = cacheConfig);

    check store.put(K1, [K1SM1, K1M1]);

    check assertSystemMessage(store, K1, K1SM1);
    check assertAllMessages(store, K1, [K1SM1, K1M1]);

    final readonly & ai:ChatSystemMessage k1sm2 = {
        role: ai:SYSTEM,
        content: "You are a helpful assistant that is aware of sports."
    };
    check store.put(K1, [k1sm2, k1m2]);

    check assertSystemMessage(store, K1, k1sm2);
    check assertAllMessages(store, K1, [k1sm2, K1M1, k1m2]);
}

@test:Config {
    before: cleanupKeys
}
function testCacheInvalidationOnRemoveAll() returns error? {
    dynamodb:Client cl = getClient();
    cache:CacheConfig cacheConfig = {
        capacity: 10,
        evictionFactor: 0.2
    };
    ShortTermMemoryStore store = check new (cl, cacheConfig = cacheConfig);

    check store.put(K1, K1SM1);
    check store.put(K1, K1M1);
    check store.put(K1, k1m2);

    check assertAllMessages(store, K1, [K1SM1, K1M1, k1m2]);

    check store.removeAll(K1);

    check assertAllMessages(store, K1, []);
    check assertSystemMessage(store, K1, ());
    check assertInteractiveMessages(store, K1, []);
}

@test:Config {
    before: cleanupKeys
}
function testCacheInvalidationOnRemoveInteractiveMessages() returns error? {
    dynamodb:Client cl = getClient();
    cache:CacheConfig cacheConfig = {
        capacity: 10,
        evictionFactor: 0.2
    };
    ShortTermMemoryStore store = check new (cl, cacheConfig = cacheConfig);

    check store.put(K1, K1SM1);
    check store.put(K1, K1M1);
    check store.put(K1, k1m2);
    check store.put(K1, K1M3);

    check assertAllMessages(store, K1, [K1SM1, K1M1, k1m2, K1M3]);

    check store.removeChatInteractiveMessages(K1);

    check assertAllMessages(store, K1, [K1SM1]);
    check assertSystemMessage(store, K1, K1SM1);
    check assertInteractiveMessages(store, K1, []);
}

@test:Config {
    before: cleanupKeys
}
function testCacheInvalidationOnRemoveSubsetOfInteractiveMessages() returns error? {
    dynamodb:Client cl = getClient();
    cache:CacheConfig cacheConfig = {
        capacity: 10,
        evictionFactor: 0.2
    };
    ShortTermMemoryStore store = check new (cl, cacheConfig = cacheConfig);

    check store.put(K1, K1SM1);
    check store.put(K1, K1M1);
    check store.put(K1, k1m2);
    check store.put(K1, K1M3);
    check store.put(K1, K1M4);

    check assertAllMessages(store, K1, [K1SM1, K1M1, k1m2, K1M3, K1M4]);

    check store.removeChatInteractiveMessages(K1, 2);

    check assertAllMessages(store, K1, [K1SM1, K1M3, K1M4]);
    check assertInteractiveMessages(store, K1, [K1M3, K1M4]);
}

@test:Config {
    before: cleanupKeys
}
function testCacheUpdateOnRemoveSystemMessage() returns error? {
    dynamodb:Client cl = getClient();
    cache:CacheConfig cacheConfig = {
        capacity: 10,
        evictionFactor: 0.2
    };
    ShortTermMemoryStore store = check new (cl, cacheConfig = cacheConfig);

    check store.put(K1, K1SM1);
    check store.put(K1, K1M1);
    check store.put(K1, k1m2);

    check assertAllMessages(store, K1, [K1SM1, K1M1, k1m2]);
    check assertSystemMessage(store, K1, K1SM1);

    check store.removeChatSystemMessage(K1);

    check assertAllMessages(store, K1, [K1M1, k1m2]);
    check assertSystemMessage(store, K1, ());
    check assertInteractiveMessages(store, K1, [K1M1, k1m2]);
}

@test:Config {
    before: cleanupKeys
}
function testCacheWithMultipleKeys() returns error? {
    dynamodb:Client cl = getClient();
    cache:CacheConfig cacheConfig = {
        capacity: 10,
        evictionFactor: 0.2
    };
    ShortTermMemoryStore store = check new (cl, cacheConfig = cacheConfig);

    check store.put(K1, K1SM1);
    check store.put(K1, K1M1);
    check store.put(K1, k1m2);

    check store.put(K2, K2M1);

    check assertAllMessages(store, K1, [K1SM1, K1M1, k1m2]);
    check assertAllMessages(store, K2, [K2M1]);

    check store.removeAll(K1);

    check assertAllMessages(store, K1, []);
    check assertAllMessages(store, K2, [K2M1]);
}

@test:Config {
    before: cleanupKeys
}
function testCacheWithSmallCapacity() returns error? {
    dynamodb:Client cl = getClient();
    cache:CacheConfig cacheConfig = {
        capacity: 2,
        evictionFactor: 0.5
    };
    ShortTermMemoryStore store = check new (cl, cacheConfig = cacheConfig);

    check store.put(K1, K1M1);
    check store.put(K2, K2M1);
    check store.put(K3, K1M3);

    check assertAllMessages(store, K1, [K1M1]);
    check assertAllMessages(store, K2, [K2M1]);

    check assertAllMessages(store, K3, [K1M3]);

    check assertAllMessages(store, K1, [K1M1]);
    check assertAllMessages(store, K2, [K2M1]);
    check assertAllMessages(store, K3, [K1M3]);
}

@test:Config {
    before: cleanupKeys
}
function testSystemMessageRetrievalDoesNotPopulateCache() returns error? {
    dynamodb:Client cl = getClient();
    cache:CacheConfig cacheConfig = {
        capacity: 10,
        evictionFactor: 0.2
    };
    ShortTermMemoryStore store = check new (cl, cacheConfig = cacheConfig);

    check store.put(K1, K1SM1);
    check store.put(K1, K1M1);
    check store.put(K1, k1m2);

    // Retrieve only system message - should NOT populate cache.
    check assertSystemMessage(store, K1, K1SM1);

    // Add more messages.
    check store.put(K1, K1M3);

    // Retrieve all messages - should load from DynamoDB and include K1M3.
    check assertAllMessages(store, K1, [K1SM1, K1M1, k1m2, K1M3]);
}

// isFull() tests

@test:Config {
    before: cleanupKeys
}
function testIsFullReturnsFalseWhenEmpty() returns error? {
    dynamodb:Client cl = getClient();
    ShortTermMemoryStore store = check new (cl, 3);

    boolean full = check store.isFull(K1);
    test:assertFalse(full);
}

@test:Config {
    before: cleanupKeys
}
function testIsFullReturnsFalseWhenBelowLimit() returns error? {
    dynamodb:Client cl = getClient();
    ShortTermMemoryStore store = check new (cl, 3);

    check store.put(K1, K1M1);
    check store.put(K1, k1m2);

    boolean full = check store.isFull(K1);
    test:assertFalse(full);
}

@test:Config {
    before: cleanupKeys
}
function testIsFullReturnsTrueWhenAtLimit() returns error? {
    dynamodb:Client cl = getClient();
    ShortTermMemoryStore store = check new (cl, 2);

    check store.put(K1, K1M1);
    check store.put(K1, k1m2);

    boolean full = check store.isFull(K1);
    test:assertTrue(full);
}

@test:Config {
    before: cleanupKeys
}
function testIsFullWithCache() returns error? {
    dynamodb:Client cl = getClient();
    cache:CacheConfig cacheConfig = {capacity: 10, evictionFactor: 0.2};
    ShortTermMemoryStore store = check new (cl, 2, cacheConfig);

    check store.put(K1, K1M1);
    check store.put(K1, k1m2);

    // Load into cache first.
    _ = check store.getAll(K1);

    // isFull counts the interactive items directly in DynamoDB, not from the cache.
    boolean full = check store.isFull(K1);
    test:assertTrue(full);
}

// getCapacity() tests

@test:Config {}
function testGetCapacityReturnsDefaultValue() returns error? {
    dynamodb:Client cl = getClient();
    ShortTermMemoryStore store = check new (cl);

    test:assertEquals(store.getCapacity(), 20);
}

@test:Config {}
function testGetCapacityReturnsCustomValue() returns error? {
    dynamodb:Client cl = getClient();
    ShortTermMemoryStore store = check new (cl, 5);

    test:assertEquals(store.getCapacity(), 5);
}

// Custom tableName tests

@test:Config {}
function testCustomTableNameStoresUnderCorrectTable() returns error? {
    dynamodb:Client cl = getClient();
    string customTable = "custom_memory_table";
    check deleteAllForKey(cl, customTable, K1);
    ShortTermMemoryStore store = check new (cl, tableName = customTable);

    check store.put(K1, K1SM1);
    check store.put(K1, K1M1);

    dynamodb:ItemGetOutput sysResult = check cl->getItem({
        TableName: customTable,
        Key: itemKey(K1, SYSTEM_MESSAGE_ID),
        ConsistentRead: true
    });
    test:assertTrue(sysResult?.Item is map<dynamodb:AttributeValue>);

    map<dynamodb:AttributeValue>[] interactiveItems = check queryItems(cl, {
        TableName: customTable,
        ConsistentRead: true,
        KeyConditionExpression: "#pk = :pk and begins_with(#sk, :prefix)",
        ExpressionAttributeNames: {"#pk": PARTITION_KEY_ATTRIBUTE, "#sk": SORT_KEY_ATTRIBUTE},
        ExpressionAttributeValues: {":pk": {S: K1}, ":prefix": {S: INTERACTIVE_ID_PREFIX}}
    });
    test:assertEquals(interactiveItems.length(), 1);

    check deleteAllForKey(cl, customTable, K1);
}

@test:Config {}
function testTwoStoresWithDifferentTablesAreIsolated() returns error? {
    dynamodb:Client cl = getClient();
    string tableA = "memory_table_a";
    string tableB = "memory_table_b";
    check deleteAllForKey(cl, tableA, K1);
    check deleteAllForKey(cl, tableB, K1);
    ShortTermMemoryStore storeA = check new (cl, tableName = tableA);
    ShortTermMemoryStore storeB = check new (cl, tableName = tableB);

    check storeA.put(K1, K1M1);
    check storeB.put(K1, K2M1);

    check assertInteractiveMessages(storeA, K1, [K1M1]);
    check assertInteractiveMessages(storeB, K1, [K2M1]);

    check deleteAllForKey(cl, tableA, K1);
    check deleteAllForKey(cl, tableB, K1);
}

// Operations on non-existent keys

@test:Config {
    before: cleanupKeys
}
function testGetAllOnNonExistentKey() returns error? {
    dynamodb:Client cl = getClient();
    ShortTermMemoryStore store = check new (cl);

    check assertAllMessages(store, K3, []);
}

@test:Config {
    before: cleanupKeys
}
function testRemoveAllOnNonExistentKey() returns error? {
    dynamodb:Client cl = getClient();
    ShortTermMemoryStore store = check new (cl);

    Error? result = store.removeAll(K3);
    test:assertTrue(result is ());
}

@test:Config {
    before: cleanupKeys
}
function testRemoveSystemMessageOnNonExistentKey() returns error? {
    dynamodb:Client cl = getClient();
    ShortTermMemoryStore store = check new (cl);

    Error? result = store.removeChatSystemMessage(K3);
    test:assertTrue(result is ());
}

@test:Config {
    before: cleanupKeys
}
function testRemoveInteractiveOnNonExistentKey() returns error? {
    dynamodb:Client cl = getClient();
    ShortTermMemoryStore store = check new (cl);

    Error? result = store.removeChatInteractiveMessages(K3);
    test:assertTrue(result is ());
}

// removeChatInteractiveMessages count edge cases

@test:Config {
    before: cleanupKeys
}
function testRemoveInteractiveWithCountZero() returns error? {
    dynamodb:Client cl = getClient();
    ShortTermMemoryStore store = check new (cl);

    check store.put(K1, K1M1);
    check store.put(K1, k1m2);

    check store.removeChatInteractiveMessages(K1, 0);

    check assertInteractiveMessages(store, K1, [K1M1, k1m2]);
    check assertFromDynamoDb(cl, K1, [K1M1, k1m2], INTERACTIVE);
}

@test:Config {
    before: cleanupKeys
}
function testRemoveInteractiveWithCountExceedingLength() returns error? {
    dynamodb:Client cl = getClient();
    ShortTermMemoryStore store = check new (cl);

    check store.put(K1, K1M1);
    check store.put(K1, k1m2);

    // count=10 exceeds 2 actual messages - should remove all.
    check store.removeChatInteractiveMessages(K1, 10);

    check assertInteractiveMessages(store, K1, []);
    check assertFromDynamoDb(cl, K1, [], INTERACTIVE);
}

// put() with empty array

@test:Config {
    before: cleanupKeys
}
function testPutAllWithEmptyArray() returns error? {
    dynamodb:Client cl = getClient();
    ShortTermMemoryStore store = check new (cl);

    check store.put(K1, K1M1);
    check store.put(K1, []);

    check assertAllMessages(store, K1, [K1M1]);
    check assertFromDynamoDb(cl, K1, [K1M1]);
}

// ai:Prompt content type tests

isolated function createTestPrompt(string[] & readonly strings, anydata[] & readonly insertions)
        returns readonly & ai:Prompt => isolated object ai:Prompt {
    public final string[] & readonly strings = strings;
    public final anydata[] & readonly insertions = insertions;
};

@test:Config {
    before: cleanupKeys
}
function testPutAndGetUserMessageWithPromptContent() returns error? {
    dynamodb:Client cl = getClient();
    ShortTermMemoryStore store = check new (cl);

    string[] & readonly strings = ["Hello, my name is ", "."];
    anydata[] & readonly insertions = ["Alice"];
    final readonly & ai:Prompt prompt = createTestPrompt(strings, insertions);
    final readonly & ai:ChatUserMessage msgWithPrompt = {role: ai:USER, content: prompt};

    check store.put(K1, msgWithPrompt);

    check assertInteractiveMessages(store, K1, [msgWithPrompt]);
    check assertFromDynamoDb(cl, K1, [msgWithPrompt], INTERACTIVE);
}

@test:Config {
    before: cleanupKeys
}
function testPutAndGetSystemMessageWithPromptContent() returns error? {
    dynamodb:Client cl = getClient();
    ShortTermMemoryStore store = check new (cl);

    string[] & readonly strings = ["You are a ", " assistant."];
    anydata[] & readonly insertions = ["helpful"];
    final readonly & ai:Prompt prompt = createTestPrompt(strings, insertions);
    final readonly & ai:ChatSystemMessage sysMsgWithPrompt = {role: ai:SYSTEM, content: prompt};

    check store.put(K1, sysMsgWithPrompt);

    check assertSystemMessage(store, K1, sysMsgWithPrompt);
    check assertFromDynamoDb(cl, K1, [sysMsgWithPrompt], SYSTEM);
}

// name field on messages

@test:Config {
    before: cleanupKeys
}
function testPutAndGetMessageWithNameField() returns error? {
    dynamodb:Client cl = getClient();
    ShortTermMemoryStore store = check new (cl);

    final readonly & ai:ChatSystemMessage namedSystem = {role: ai:SYSTEM, content: "You are helpful.", name: "system_v2"};
    final readonly & ai:ChatUserMessage namedUser = {role: ai:USER, content: "Hi there", name: "alice"};

    check store.put(K1, namedSystem);
    check store.put(K1, namedUser);

    check assertSystemMessage(store, K1, namedSystem);
    check assertInteractiveMessages(store, K1, [namedUser]);
    check assertFromDynamoDb(cl, K1, [namedSystem], SYSTEM);
    check assertFromDynamoDb(cl, K1, [namedUser], INTERACTIVE);
}
