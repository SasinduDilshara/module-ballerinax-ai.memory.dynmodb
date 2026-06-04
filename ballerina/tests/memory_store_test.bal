// Copyright (c) 2026, WSO2 LLC. (http://www.wso2.com).
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

// Unit tests for `ShortTermMemoryStore`.
//
// The tests run entirely against an in-memory `FakeDynamoDbClient`
// (`tests/fake_dynamodb_client.bal`) that is installed via
// `test:mock(dynamodb:Client, fake)`. No AWS credentials or DynamoDB Local
// container are required.

import ballerina/ai;
import ballerina/cache;
import ballerina/test;
import ballerinax/aws.dynamodb;

// -----------------------------------------------------------------------------
// Common fixtures
// -----------------------------------------------------------------------------

const string K1 = "key1";
const string K2 = "key2";
const string K3 = "key3";

const string TABLE_NAME = "chat_memory";

final readonly & ai:ChatSystemMessage SYSTEM_WEATHER = {
    role: ai:SYSTEM,
    content: "You are a helpful assistant that is aware of the weather."
};
final readonly & ai:ChatSystemMessage SYSTEM_SPORTS = {
    role: ai:SYSTEM,
    content: "You are a helpful assistant that is aware of sports."
};

final readonly & ai:ChatUserMessage USER_INTRO = {role: ai:USER, content: "Hello, my name is Alice. I'm from Seattle."};
final readonly & ai:ChatAssistantMessage ASSISTANT_GREETING = {
    role: ai:ASSISTANT,
    content: "Hello Alice, what can I do for you?"
};
final readonly & ai:ChatUserMessage USER_WEATHER_Q = {role: ai:USER, content: "I would like to know the weather today."};
final readonly & ai:ChatAssistantMessage ASSISTANT_WEATHER_A = {
    role: ai:ASSISTANT,
    content: "The weather in Seattle today is mostly cloudy with occasional showers and a high around 58°F."
};
final readonly & ai:ChatUserMessage USER_K2 = {role: ai:USER, content: "Hello, my name is Bob."};

// Builds a fresh storage and the corresponding mocked `dynamodb:Client` used as
// the dependency injected into a store under test. Each test calls this to
// rotate the (module-level) active storage so state never leaks between tests.
function newFakePair() returns [FakeStorage, dynamodb:Client] {
    FakeStorage fake = newFakeStorage();
    dynamodb:Client mocked = test:mock(dynamodb:Client, new FakeDynamoDbClient());
    return [fake, mocked];
}

// -----------------------------------------------------------------------------
// Table lifecycle / initialization
// -----------------------------------------------------------------------------

@test:Config {}
function testInitCreatesTableWhenAbsent() returns error? {
    var [fake, mocked] = newFakePair();
    test:assertFalse(fake.hasTable(TABLE_NAME), "Table should not exist before store init");

    _ = check new ShortTermMemoryStore(mocked);

    test:assertTrue(fake.hasTable(TABLE_NAME),
        "Store init must create the backing DynamoDB table when it does not exist");
}

@test:Config {}
function testInitReusesExistingTable() returns error? {
    var [_, mocked] = newFakePair();
    // First init creates the table; the second init must succeed without errors,
    // because the connector handles the `ResourceInUseException` returned by the
    // fake the same way the real DynamoDB does.
    _ = check new ShortTermMemoryStore(mocked);
    _ = check new ShortTermMemoryStore(mocked);
}

@test:Config {}
function testInitRejectsInvalidTableName() returns error? {
    var [_, mocked] = newFakePair();
    string[] invalidNames = ["ab", "has space", "exclaim!", "two/parts"];
    foreach string name in invalidNames {
        Error|ShortTermMemoryStore result = new ShortTermMemoryStore(mocked, tableConfig = {tableName: name});
        test:assertTrue(result is Error,
            string `Expected init to fail for invalid table name '${name}'`);
    }
}

@test:Config {}
function testInitAcceptsValidTableName() returns error? {
    var [fake, mocked] = newFakePair();
    string customTable = "custom.memory-table_1";
    _ = check new ShortTermMemoryStore(mocked, tableConfig = {tableName: customTable});
    test:assertTrue(fake.hasTable(customTable),
        string `Store must create the requested custom table '${customTable}'`);
}

@test:Config {}
function testGetCapacityDefault() returns error? {
    var [_, mocked] = newFakePair();
    ShortTermMemoryStore store = check new (mocked);
    test:assertEquals(store.getCapacity(), 20);
}

@test:Config {}
function testGetCapacityCustom() returns error? {
    var [_, mocked] = newFakePair();
    ShortTermMemoryStore store = check new (mocked, 7);
    test:assertEquals(store.getCapacity(), 7);
}

// -----------------------------------------------------------------------------
// Happy paths: put + get for system, interactive, and combined messages.
// -----------------------------------------------------------------------------

@test:Config {}
function testPutAndGetSystemMessage() returns error? {
    var [_, mocked] = newFakePair();
    ShortTermMemoryStore store = check new (mocked);

    check store.put(K1, SYSTEM_WEATHER);

    ai:ChatSystemMessage? actual = check store.getChatSystemMessage(K1);
    test:assertTrue(actual is ai:ChatSystemMessage, "Expected a system message to be returned");
    assertChatMessageEquals(<ai:ChatMessage>actual, SYSTEM_WEATHER);
}

@test:Config {}
function testGetSystemMessageReturnsNilWhenAbsent() returns error? {
    var [_, mocked] = newFakePair();
    ShortTermMemoryStore store = check new (mocked);

    ai:ChatSystemMessage? actual = check store.getChatSystemMessage(K1);
    test:assertTrue(actual is (), "Expected nil when no system message is set");
}

@test:Config {}
function testPutAndGetInteractiveMessagesPreservesOrder() returns error? {
    var [_, mocked] = newFakePair();
    ShortTermMemoryStore store = check new (mocked);

    check store.put(K1, USER_INTRO);
    check store.put(K1, ASSISTANT_GREETING);
    check store.put(K1, USER_WEATHER_Q);
    check store.put(K1, ASSISTANT_WEATHER_A);

    check assertInteractiveMessages(store, K1,
        [USER_INTRO, ASSISTANT_GREETING, USER_WEATHER_Q, ASSISTANT_WEATHER_A]);
}

@test:Config {}
function testGetAllCombinesSystemAndInteractive() returns error? {
    var [_, mocked] = newFakePair();
    ShortTermMemoryStore store = check new (mocked);

    check store.put(K1, SYSTEM_WEATHER);
    check store.put(K1, USER_INTRO);
    check store.put(K1, ASSISTANT_GREETING);

    check assertAllMessages(store, K1, [SYSTEM_WEATHER, USER_INTRO, ASSISTANT_GREETING]);
}

@test:Config {}
function testGetAllReturnsOnlyInteractiveWhenNoSystem() returns error? {
    var [_, mocked] = newFakePair();
    ShortTermMemoryStore store = check new (mocked);

    check store.put(K1, USER_INTRO);
    check store.put(K1, ASSISTANT_GREETING);

    ai:ChatMessage[] all = check store.getAll(K1);
    test:assertEquals(all.length(), 2);
    assertChatMessageEquals(all[0], USER_INTRO);
    assertChatMessageEquals(all[1], ASSISTANT_GREETING);
}

@test:Config {}
function testGetAllOnEmptyKey() returns error? {
    var [_, mocked] = newFakePair();
    ShortTermMemoryStore store = check new (mocked);

    check assertAllMessages(store, K3, []);
}

// -----------------------------------------------------------------------------
// put() variants: arrays, mixed message kinds, prompt content, and name fields.
// -----------------------------------------------------------------------------

@test:Config {}
function testPutAllInsertsMixedBatch() returns error? {
    var [_, mocked] = newFakePair();
    ShortTermMemoryStore store = check new (mocked);

    check store.put(K1, [SYSTEM_WEATHER, USER_INTRO, ASSISTANT_GREETING]);

    check assertAllMessages(store, K1, [SYSTEM_WEATHER, USER_INTRO, ASSISTANT_GREETING]);
}

@test:Config {}
function testPutAllWithMultipleSystemMessagesKeepsLast() returns error? {
    var [_, mocked] = newFakePair();
    ShortTermMemoryStore store = check new (mocked);

    // The store contract says: when an array contains multiple ChatSystemMessage
    // values, only the LAST one is persisted; earlier ones are discarded.
    check store.put(K1, [SYSTEM_WEATHER, USER_INTRO, ASSISTANT_GREETING, SYSTEM_SPORTS]);

    check assertSystemMessage(store, K1, SYSTEM_SPORTS);
    check assertInteractiveMessages(store, K1, [USER_INTRO, ASSISTANT_GREETING]);
}

@test:Config {}
function testPutAllWithEmptyArrayIsNoOp() returns error? {
    var [_, mocked] = newFakePair();
    ShortTermMemoryStore store = check new (mocked);

    check store.put(K1, USER_INTRO);
    check store.put(K1, []);

    check assertInteractiveMessages(store, K1, [USER_INTRO]);
}

@test:Config {}
function testPutAllWithOnlySystemMessages() returns error? {
    var [_, mocked] = newFakePair();
    ShortTermMemoryStore store = check new (mocked);

    check store.put(K1, [SYSTEM_WEATHER, SYSTEM_SPORTS]);

    check assertSystemMessage(store, K1, SYSTEM_SPORTS);
    check assertInteractiveMessages(store, K1, []);
}

@test:Config {}
function testPutSystemMessageOverwrites() returns error? {
    var [_, mocked] = newFakePair();
    ShortTermMemoryStore store = check new (mocked);

    check store.put(K1, SYSTEM_WEATHER);
    check store.put(K1, USER_INTRO);
    check store.put(K1, SYSTEM_SPORTS);

    check assertSystemMessage(store, K1, SYSTEM_SPORTS);
    check assertInteractiveMessages(store, K1, [USER_INTRO]);
}

@test:Config {}
function testPutFunctionAndAssistantMessages() returns error? {
    var [_, mocked] = newFakePair();
    ShortTermMemoryStore store = check new (mocked);

    final readonly & ai:ChatFunctionMessage funcMessage = {
        role: "function",
        name: "getWeather",
        id: "func1"
    };

    check store.put(K1, USER_INTRO);
    check store.put(K1, ASSISTANT_GREETING);
    check store.put(K1, funcMessage);

    check assertInteractiveMessages(store, K1, [USER_INTRO, ASSISTANT_GREETING, funcMessage]);
}

@test:Config {}
function testPutUserMessageWithNameField() returns error? {
    var [_, mocked] = newFakePair();
    ShortTermMemoryStore store = check new (mocked);

    final readonly & ai:ChatUserMessage namedUser = {role: ai:USER, content: "Hi", name: "alice"};
    check store.put(K1, namedUser);

    check assertInteractiveMessages(store, K1, [namedUser]);
}

@test:Config {}
function testPutSystemMessageWithNameField() returns error? {
    var [_, mocked] = newFakePair();
    ShortTermMemoryStore store = check new (mocked);

    final readonly & ai:ChatSystemMessage namedSystem =
        {role: ai:SYSTEM, content: "You are helpful.", name: "system_v2"};
    check store.put(K1, namedSystem);

    check assertSystemMessage(store, K1, namedSystem);
}

isolated function createTestPrompt(string[] & readonly strings, anydata[] & readonly insertions)
        returns readonly & ai:Prompt => isolated object ai:Prompt {
    public final string[] & readonly strings = strings;
    public final anydata[] & readonly insertions = insertions;
};

@test:Config {}
function testPutUserMessageWithPromptContent() returns error? {
    var [_, mocked] = newFakePair();
    ShortTermMemoryStore store = check new (mocked);

    string[] & readonly parts = ["Hello, my name is ", "."];
    anydata[] & readonly insertions = ["Alice"];
    final readonly & ai:Prompt prompt = createTestPrompt(parts, insertions);
    final readonly & ai:ChatUserMessage userWithPrompt = {role: ai:USER, content: prompt};

    check store.put(K1, userWithPrompt);

    check assertInteractiveMessages(store, K1, [userWithPrompt]);
}

@test:Config {}
function testPutSystemMessageWithPromptContent() returns error? {
    var [_, mocked] = newFakePair();
    ShortTermMemoryStore store = check new (mocked);

    string[] & readonly parts = ["You are a ", " assistant."];
    anydata[] & readonly insertions = ["helpful"];
    final readonly & ai:Prompt prompt = createTestPrompt(parts, insertions);
    final readonly & ai:ChatSystemMessage sysWithPrompt = {role: ai:SYSTEM, content: prompt};

    check store.put(K1, sysWithPrompt);

    check assertSystemMessage(store, K1, sysWithPrompt);
}

// -----------------------------------------------------------------------------
// Removal: system message, interactive messages, all.
// -----------------------------------------------------------------------------

@test:Config {}
function testRemoveSystemMessageLeavesInteractiveIntact() returns error? {
    var [_, mocked] = newFakePair();
    ShortTermMemoryStore store = check new (mocked);

    check store.put(K1, SYSTEM_WEATHER);
    check store.put(K1, USER_INTRO);
    check store.put(K1, ASSISTANT_GREETING);

    check store.removeChatSystemMessage(K1);

    check assertSystemMessage(store, K1, ());
    check assertInteractiveMessages(store, K1, [USER_INTRO, ASSISTANT_GREETING]);
}

@test:Config {}
function testRemoveAllInteractiveLeavesSystemIntact() returns error? {
    var [_, mocked] = newFakePair();
    ShortTermMemoryStore store = check new (mocked);

    check store.put(K1, SYSTEM_WEATHER);
    check store.put(K1, USER_INTRO);
    check store.put(K1, ASSISTANT_GREETING);

    check store.removeChatInteractiveMessages(K1);

    check assertSystemMessage(store, K1, SYSTEM_WEATHER);
    check assertInteractiveMessages(store, K1, []);
}

@test:Config {}
function testRemoveInteractivePartial() returns error? {
    var [_, mocked] = newFakePair();
    ShortTermMemoryStore store = check new (mocked);

    check store.put(K1, SYSTEM_WEATHER);
    check store.put(K1, USER_INTRO);
    check store.put(K1, ASSISTANT_GREETING);
    check store.put(K1, USER_WEATHER_Q);
    check store.put(K1, ASSISTANT_WEATHER_A);

    // Remove the first two interactive messages.
    check store.removeChatInteractiveMessages(K1, 2);

    check assertSystemMessage(store, K1, SYSTEM_WEATHER);
    check assertInteractiveMessages(store, K1, [USER_WEATHER_Q, ASSISTANT_WEATHER_A]);
}

@test:Config {}
function testRemoveInteractiveCountZeroIsNoOp() returns error? {
    var [_, mocked] = newFakePair();
    ShortTermMemoryStore store = check new (mocked);

    check store.put(K1, USER_INTRO);
    check store.put(K1, ASSISTANT_GREETING);

    check store.removeChatInteractiveMessages(K1, 0);

    check assertInteractiveMessages(store, K1, [USER_INTRO, ASSISTANT_GREETING]);
}

@test:Config {}
function testRemoveInteractiveCountExceedsLength() returns error? {
    var [_, mocked] = newFakePair();
    ShortTermMemoryStore store = check new (mocked);

    check store.put(K1, USER_INTRO);
    check store.put(K1, ASSISTANT_GREETING);

    check store.removeChatInteractiveMessages(K1, 10);

    check assertInteractiveMessages(store, K1, []);
}

@test:Config {}
function testRemoveInteractiveNegativeCountErrors() returns error? {
    var [_, mocked] = newFakePair();
    ShortTermMemoryStore store = check new (mocked);

    check store.put(K1, USER_INTRO);

    Error? result = store.removeChatInteractiveMessages(K1, -1);
    test:assertTrue(result is Error, "Negative counts must yield an error");
}

@test:Config {}
function testRemoveAllClearsEverythingForKey() returns error? {
    var [_, mocked] = newFakePair();
    ShortTermMemoryStore store = check new (mocked);

    check store.put(K1, SYSTEM_WEATHER);
    check store.put(K1, USER_INTRO);
    check store.put(K1, ASSISTANT_GREETING);

    check store.removeAll(K1);

    check assertSystemMessage(store, K1, ());
    check assertInteractiveMessages(store, K1, []);
    check assertAllMessages(store, K1, []);
}

@test:Config {}
function testRemoveSystemOnNonExistentKeyIsNoOp() returns error? {
    var [_, mocked] = newFakePair();
    ShortTermMemoryStore store = check new (mocked);

    Error? result = store.removeChatSystemMessage(K3);
    test:assertTrue(result is (), "Removing a non-existent system message must succeed");
}

@test:Config {}
function testRemoveInteractiveOnNonExistentKeyIsNoOp() returns error? {
    var [_, mocked] = newFakePair();
    ShortTermMemoryStore store = check new (mocked);

    Error? result = store.removeChatInteractiveMessages(K3);
    test:assertTrue(result is (), "Removing interactive messages on an unseen key must succeed");
}

@test:Config {}
function testRemoveAllOnNonExistentKeyIsNoOp() returns error? {
    var [_, mocked] = newFakePair();
    ShortTermMemoryStore store = check new (mocked);

    Error? result = store.removeAll(K3);
    test:assertTrue(result is (), "Removing on an unseen key must succeed");
}

@test:Config {}
function testAddingMessagesAfterRemoveAllStartsClean() returns error? {
    var [_, mocked] = newFakePair();
    ShortTermMemoryStore store = check new (mocked);

    check store.put(K1, SYSTEM_WEATHER);
    check store.put(K1, USER_INTRO);
    check store.put(K1, ASSISTANT_GREETING);
    check store.removeAll(K1);

    check store.put(K1, USER_WEATHER_Q);

    check assertSystemMessage(store, K1, ());
    check assertInteractiveMessages(store, K1, [USER_WEATHER_Q]);
}

// -----------------------------------------------------------------------------
// Multi-key isolation.
// -----------------------------------------------------------------------------

@test:Config {}
function testKeysAreIsolated() returns error? {
    var [_, mocked] = newFakePair();
    ShortTermMemoryStore store = check new (mocked);

    check store.put(K1, SYSTEM_WEATHER);
    check store.put(K1, USER_INTRO);
    check store.put(K2, USER_K2);

    check assertSystemMessage(store, K1, SYSTEM_WEATHER);
    check assertInteractiveMessages(store, K1, [USER_INTRO]);

    check assertSystemMessage(store, K2, ());
    check assertInteractiveMessages(store, K2, [USER_K2]);

    check store.removeAll(K1);

    check assertSystemMessage(store, K1, ());
    check assertInteractiveMessages(store, K1, []);

    check assertInteractiveMessages(store, K2, [USER_K2]);
}

// -----------------------------------------------------------------------------
// Custom table name behaviour.
// -----------------------------------------------------------------------------

@test:Config {}
function testCustomTableNameWritesToThatTable() returns error? {
    var [fake, mocked] = newFakePair();
    string custom = "custom_memory_table";
    ShortTermMemoryStore store = check new (mocked, tableConfig = {tableName: custom});

    check store.put(K1, SYSTEM_WEATHER);
    check store.put(K1, USER_INTRO);

    test:assertTrue(fake.hasTable(custom),
        string `Expected custom table '${custom}' to be created`);
    test:assertEquals(fake.hasTable(TABLE_NAME), false,
        "Default table name should NOT be created when a custom one is supplied");
    test:assertTrue(fake.peekBody(custom, K1, SYSTEM_MESSAGE_ID) is string,
        "System message must be persisted in the custom table");
}

@test:Config {}
function testTwoStoresOnDifferentTablesAreIsolated() returns error? {
    var [_, mocked] = newFakePair();
    ShortTermMemoryStore storeA = check new (mocked, tableConfig = {tableName: "memory_a"});
    ShortTermMemoryStore storeB = check new (mocked, tableConfig = {tableName: "memory_b"});

    check storeA.put(K1, USER_INTRO);
    check storeB.put(K1, USER_K2);

    check assertInteractiveMessages(storeA, K1, [USER_INTRO]);
    check assertInteractiveMessages(storeB, K1, [USER_K2]);
}

// -----------------------------------------------------------------------------
// isFull() and capacity boundaries.
// -----------------------------------------------------------------------------

@test:Config {}
function testIsFullFalseWhenEmpty() returns error? {
    var [_, mocked] = newFakePair();
    ShortTermMemoryStore store = check new (mocked, 3);

    test:assertFalse(check store.isFull(K1));
}

@test:Config {}
function testIsFullFalseWhenBelowCapacity() returns error? {
    var [_, mocked] = newFakePair();
    ShortTermMemoryStore store = check new (mocked, 3);

    check store.put(K1, USER_INTRO);
    check store.put(K1, ASSISTANT_GREETING);

    test:assertFalse(check store.isFull(K1));
}

@test:Config {}
function testIsFullTrueAtCapacity() returns error? {
    var [_, mocked] = newFakePair();
    ShortTermMemoryStore store = check new (mocked, 2);

    check store.put(K1, USER_INTRO);
    check store.put(K1, ASSISTANT_GREETING);

    test:assertTrue(check store.isFull(K1));
}

@test:Config {}
function testIsFullTrueAboveCapacity() returns error? {
    var [_, mocked] = newFakePair();
    // The store does not enforce the cap on put — `isFull` is purely advisory.
    ShortTermMemoryStore store = check new (mocked, 2);

    check store.put(K1, USER_INTRO);
    check store.put(K1, ASSISTANT_GREETING);
    check store.put(K1, USER_WEATHER_Q);

    test:assertTrue(check store.isFull(K1));
}

@test:Config {}
function testIsFullIgnoresSystemMessage() returns error? {
    var [_, mocked] = newFakePair();
    ShortTermMemoryStore store = check new (mocked, 2);

    check store.put(K1, SYSTEM_WEATHER);
    check store.put(K1, USER_INTRO);

    test:assertFalse(check store.isFull(K1),
        "isFull must not count the system message towards capacity");
}

// -----------------------------------------------------------------------------
// Cache behaviour.
// -----------------------------------------------------------------------------

final cache:CacheConfig CACHE_CONFIG = {capacity: 10, evictionFactor: 0.2};

@test:Config {}
function testCacheServesAfterFirstLoad() returns error? {
    var [_, mocked] = newFakePair();
    ShortTermMemoryStore store = check new (mocked, cacheConfig = CACHE_CONFIG);

    check store.put(K1, SYSTEM_WEATHER);
    check store.put(K1, USER_INTRO);
    check store.put(K1, ASSISTANT_GREETING);

    // First read populates the cache; subsequent reads must remain consistent.
    check assertAllMessages(store, K1, [SYSTEM_WEATHER, USER_INTRO, ASSISTANT_GREETING]);
    check assertAllMessages(store, K1, [SYSTEM_WEATHER, USER_INTRO, ASSISTANT_GREETING]);
    check assertInteractiveMessages(store, K1, [USER_INTRO, ASSISTANT_GREETING]);
}

@test:Config {}
function testCacheReflectsSubsequentPuts() returns error? {
    var [_, mocked] = newFakePair();
    ShortTermMemoryStore store = check new (mocked, cacheConfig = CACHE_CONFIG);

    check store.put(K1, SYSTEM_WEATHER);
    check store.put(K1, USER_INTRO);
    check assertAllMessages(store, K1, [SYSTEM_WEATHER, USER_INTRO]);

    check store.put(K1, ASSISTANT_GREETING);
    check store.put(K1, USER_WEATHER_Q);

    check assertAllMessages(store, K1, [SYSTEM_WEATHER, USER_INTRO, ASSISTANT_GREETING, USER_WEATHER_Q]);
}

@test:Config {}
function testCacheReflectsPutAllAfterLoad() returns error? {
    var [_, mocked] = newFakePair();
    ShortTermMemoryStore store = check new (mocked, cacheConfig = CACHE_CONFIG);

    check store.put(K1, [SYSTEM_WEATHER, USER_INTRO]);
    check assertAllMessages(store, K1, [SYSTEM_WEATHER, USER_INTRO]);

    check store.put(K1, [ASSISTANT_GREETING, USER_WEATHER_Q]);

    check assertAllMessages(store, K1, [SYSTEM_WEATHER, USER_INTRO, ASSISTANT_GREETING, USER_WEATHER_Q]);
}

@test:Config {}
function testCacheReflectsSystemMessageOverwrite() returns error? {
    var [_, mocked] = newFakePair();
    ShortTermMemoryStore store = check new (mocked, cacheConfig = CACHE_CONFIG);

    check store.put(K1, SYSTEM_WEATHER);
    check store.put(K1, USER_INTRO);
    check assertAllMessages(store, K1, [SYSTEM_WEATHER, USER_INTRO]);

    check store.put(K1, SYSTEM_SPORTS);

    check assertAllMessages(store, K1, [SYSTEM_SPORTS, USER_INTRO]);
}

@test:Config {}
function testCacheReflectsRemoveAll() returns error? {
    var [_, mocked] = newFakePair();
    ShortTermMemoryStore store = check new (mocked, cacheConfig = CACHE_CONFIG);

    check store.put(K1, SYSTEM_WEATHER);
    check store.put(K1, USER_INTRO);
    check assertAllMessages(store, K1, [SYSTEM_WEATHER, USER_INTRO]);

    check store.removeAll(K1);

    check assertSystemMessage(store, K1, ());
    check assertInteractiveMessages(store, K1, []);
}

@test:Config {}
function testCacheReflectsRemoveSystemMessage() returns error? {
    var [_, mocked] = newFakePair();
    ShortTermMemoryStore store = check new (mocked, cacheConfig = CACHE_CONFIG);

    check store.put(K1, SYSTEM_WEATHER);
    check store.put(K1, USER_INTRO);
    check assertAllMessages(store, K1, [SYSTEM_WEATHER, USER_INTRO]);

    check store.removeChatSystemMessage(K1);

    check assertSystemMessage(store, K1, ());
    check assertInteractiveMessages(store, K1, [USER_INTRO]);
}

@test:Config {}
function testCacheReflectsRemoveAllInteractive() returns error? {
    var [_, mocked] = newFakePair();
    ShortTermMemoryStore store = check new (mocked, cacheConfig = CACHE_CONFIG);

    check store.put(K1, SYSTEM_WEATHER);
    check store.put(K1, USER_INTRO);
    check store.put(K1, ASSISTANT_GREETING);
    check assertAllMessages(store, K1, [SYSTEM_WEATHER, USER_INTRO, ASSISTANT_GREETING]);

    check store.removeChatInteractiveMessages(K1);

    check assertSystemMessage(store, K1, SYSTEM_WEATHER);
    check assertInteractiveMessages(store, K1, []);
}

@test:Config {}
function testCacheReflectsPartialInteractiveRemoval() returns error? {
    var [_, mocked] = newFakePair();
    ShortTermMemoryStore store = check new (mocked, cacheConfig = CACHE_CONFIG);

    check store.put(K1, SYSTEM_WEATHER);
    check store.put(K1, USER_INTRO);
    check store.put(K1, ASSISTANT_GREETING);
    check store.put(K1, USER_WEATHER_Q);
    check store.put(K1, ASSISTANT_WEATHER_A);

    check assertAllMessages(store, K1,
        [SYSTEM_WEATHER, USER_INTRO, ASSISTANT_GREETING, USER_WEATHER_Q, ASSISTANT_WEATHER_A]);

    check store.removeChatInteractiveMessages(K1, 2);

    check assertInteractiveMessages(store, K1, [USER_WEATHER_Q, ASSISTANT_WEATHER_A]);
}

@test:Config {}
function testCacheWithSmallCapacityEvictsLRU() returns error? {
    var [_, mocked] = newFakePair();
    cache:CacheConfig tinyCache = {capacity: 2, evictionFactor: 0.5};
    ShortTermMemoryStore store = check new (mocked, cacheConfig = tinyCache);

    check store.put(K1, USER_INTRO);
    check store.put(K2, USER_K2);
    check store.put(K3, USER_WEATHER_Q);

    // Even after eviction, the underlying store still returns the right data.
    check assertInteractiveMessages(store, K1, [USER_INTRO]);
    check assertInteractiveMessages(store, K2, [USER_K2]);
    check assertInteractiveMessages(store, K3, [USER_WEATHER_Q]);
}

@test:Config {}
function testCacheNotPopulatedBySystemMessageFetch() returns error? {
    var [_, mocked] = newFakePair();
    ShortTermMemoryStore store = check new (mocked, cacheConfig = CACHE_CONFIG);

    check store.put(K1, SYSTEM_WEATHER);
    check store.put(K1, USER_INTRO);

    // Pulling only the system message must NOT prime the cache; otherwise the
    // next interactive-message put would either be dropped or duplicated.
    check assertSystemMessage(store, K1, SYSTEM_WEATHER);

    check store.put(K1, ASSISTANT_GREETING);

    check assertAllMessages(store, K1, [SYSTEM_WEATHER, USER_INTRO, ASSISTANT_GREETING]);
}

@test:Config {}
function testIsFullStillUsesDynamoDbWhenCacheEnabled() returns error? {
    var [_, mocked] = newFakePair();
    ShortTermMemoryStore store = check new (mocked, 2, CACHE_CONFIG);

    check store.put(K1, USER_INTRO);
    check store.put(K1, ASSISTANT_GREETING);
    _ = check store.getAll(K1);

    test:assertTrue(check store.isFull(K1));
}

// -----------------------------------------------------------------------------
// Insertion-order sanity (many interactive messages).
// -----------------------------------------------------------------------------

@test:Config {}
function testManyInteractiveMessagesPreserveOrder() returns error? {
    var [_, mocked] = newFakePair();
    ShortTermMemoryStore store = check new (mocked, 100);

    ai:ChatUserMessage[] inserted = [];
    foreach int i in 0 ..< 30 {
        ai:ChatUserMessage msg = {role: ai:USER, content: string `message-${i}`};
        check store.put(K1, msg);
        inserted.push(msg);
    }

    ai:ChatInteractiveMessage[] readBack = check store.getChatInteractiveMessages(K1);
    test:assertEquals(readBack.length(), inserted.length());
    foreach int i in 0 ..< inserted.length() {
        assertChatMessageEquals(readBack[i], inserted[i]);
    }
}

@test:Config {}
function testPutAllAppendBatchPreservesOrder() returns error? {
    var [_, mocked] = newFakePair();
    ShortTermMemoryStore store = check new (mocked, 100);

    ai:ChatMessage[] firstBatch = [];
    foreach int i in 0 ..< 5 {
        firstBatch.push({role: ai:USER, content: string `first-${i}`});
    }
    ai:ChatMessage[] secondBatch = [];
    foreach int i in 0 ..< 5 {
        secondBatch.push({role: ai:USER, content: string `second-${i}`});
    }

    check store.put(K1, firstBatch);
    check store.put(K1, secondBatch);

    ai:ChatMessage[] combined = [...firstBatch, ...secondBatch];
    ai:ChatInteractiveMessage[] readBack = check store.getChatInteractiveMessages(K1);
    test:assertEquals(readBack.length(), combined.length());
    foreach int i in 0 ..< combined.length() {
        assertChatMessageEquals(readBack[i], combined[i]);
    }
}

// -----------------------------------------------------------------------------
// Control-plane / retry branches.
//
// These paths cannot be reached against DynamoDB Local (which creates tables
// instantly and never throttles a BatchWriteItem), so they are exercised here
// by arming the fault-injection knobs on `FakeDynamoDbClient`.
// -----------------------------------------------------------------------------

@test:Config {}
function testInitWaitsForTableToBecomeActive() returns error? {
    var [fake, mocked] = newFakePair();
    // Force the first DescribeTable *after* creation to report CREATING; the
    // store must poll again (sleeping between polls) until it sees ACTIVE before
    // init returns. One armed poll is enough to drive the loop's wait branch.
    fake.setActivationPolls(1);

    ShortTermMemoryStore store = check new ShortTermMemoryStore(mocked);

    test:assertTrue(fake.hasTable(TABLE_NAME), "Table must be created during init");
    // The store is fully usable the instant init returns, which proves init did
    // not return until the table reported ACTIVE.
    check store.put(K1, USER_INTRO);
    check assertInteractiveMessages(store, K1, [USER_INTRO]);
}

@test:Config {}
function testBatchWriteRetriesUnprocessedItems() returns error? {
    var [fake, mocked] = newFakePair();
    ShortTermMemoryStore store = check new (mocked);
    // The first BatchWriteItem reports every item as unprocessed; the store must
    // retry the chunk and ultimately persist all three messages in order.
    fake.setUnprocessedRounds(1);

    check store.put(K1, [USER_INTRO, ASSISTANT_GREETING, USER_WEATHER_Q]);

    check assertInteractiveMessages(store, K1, [USER_INTRO, ASSISTANT_GREETING, USER_WEATHER_Q]);
}

@test:Config {}
function testBatchWriteFailsAfterMaxRetries() returns error? {
    var [fake, mocked] = newFakePair();
    ShortTermMemoryStore store = check new (mocked);
    // Every attempt keeps reporting unprocessed items, so the chunk never drains.
    // The store must give up after MAX_BATCH_WRITE_RETRIES and surface an error
    // rather than silently dropping the writes. (100 > the 1 + MAX retries the
    // store performs, so the fault outlives every attempt.)
    fake.setUnprocessedRounds(100);

    Error? result = store.put(K1, [USER_INTRO, ASSISTANT_GREETING]);
    test:assertTrue(result is Error, "put must fail when batch writes never drain");
}

@test:Config {}
function testInitRejectsProvisionedWithNonPositiveCapacity() returns error? {
    var [_, mocked] = newFakePair();
    // Under PROVISIONED billing the store validates that both capacities are
    // positive integers, returning an error before any AWS call is made.
    Error|ShortTermMemoryStore zeroRead = new (mocked, tableConfig = {
        billingMode: dynamodb:PROVISIONED, readCapacityUnits: 0, writeCapacityUnits: 5
    });
    test:assertTrue(zeroRead is Error, "Zero readCapacityUnits under PROVISIONED must be rejected");

    Error|ShortTermMemoryStore zeroWrite = new (mocked, tableConfig = {
        billingMode: dynamodb:PROVISIONED, readCapacityUnits: 5, writeCapacityUnits: 0
    });
    test:assertTrue(zeroWrite is Error, "Zero writeCapacityUnits under PROVISIONED must be rejected");

    Error|ShortTermMemoryStore negative = new (mocked, tableConfig = {
        billingMode: dynamodb:PROVISIONED, readCapacityUnits: -1, writeCapacityUnits: -1
    });
    test:assertTrue(negative is Error, "Negative capacities under PROVISIONED must be rejected");
}

@test:Config {}
function testInitProvisionedWithValidCapacityCreatesTable() returns error? {
    var [fake, mocked] = newFakePair();
    // The positive-capacity PROVISIONED path must pass validation and create the
    // table (the throughput is carried on the CreateTable input).
    _ = check new ShortTermMemoryStore(mocked, tableConfig = {
        billingMode: dynamodb:PROVISIONED, readCapacityUnits: 3, writeCapacityUnits: 2
    });
    test:assertTrue(fake.hasTable(TABLE_NAME), "PROVISIONED table with valid capacities must be created");
}

@test:Config {}
function testInitWithCreateTableIfNotExistsFalseSkipsCreate() returns error? {
    var [fake, mocked] = newFakePair();
    test:assertFalse(fake.hasTable(TABLE_NAME), "Precondition: table must be absent");

    // With auto-create disabled the store performs no control-plane calls during
    // init: it neither describes nor creates the table.
    _ = check new ShortTermMemoryStore(mocked, tableConfig = {createTableIfNotExists: false});

    test:assertFalse(fake.hasTable(TABLE_NAME),
        "init with createTableIfNotExists=false must not create the table");
}

@test:Config {}
function testInitWithCreateTableIfNotExistsFalseUsesExistingTable() returns error? {
    var [_, mocked] = newFakePair();
    // Provision the table out of band first (as IaC would).
    _ = check new ShortTermMemoryStore(mocked);

    // A second store with auto-create disabled assumes the table already exists
    // and operates against it without any control-plane call.
    ShortTermMemoryStore store = check new (mocked, tableConfig = {createTableIfNotExists: false});
    check store.put(K1, USER_INTRO);
    check assertInteractiveMessages(store, K1, [USER_INTRO]);
}

// -----------------------------------------------------------------------------
// Shared assertions.
// -----------------------------------------------------------------------------

function assertAllMessages(ShortTermMemoryStore store, string key, ai:ChatMessage[] expected) returns error? {
    ai:ChatMessage[] actual = check store.getAll(key);
    test:assertEquals(actual.length(), expected.length(),
        string `getAll(${key}) length mismatch`);
    foreach int i in 0 ..< actual.length() {
        assertChatMessageEquals(actual[i], expected[i]);
    }
}

function assertSystemMessage(ShortTermMemoryStore store, string key, ai:ChatSystemMessage? expected) returns error? {
    ai:ChatSystemMessage? actual = check store.getChatSystemMessage(key);
    if expected is () && actual is () {
        return;
    }
    if expected is () || actual is () {
        test:assertFail(string `getChatSystemMessage(${key}) presence mismatch`);
    }
    assertChatMessageEquals(actual, expected);
}

function assertInteractiveMessages(ShortTermMemoryStore store, string key,
        ai:ChatInteractiveMessage[] expected) returns error? {
    ai:ChatInteractiveMessage[] actual = check store.getChatInteractiveMessages(key);
    test:assertEquals(actual.length(), expected.length(),
        string `getChatInteractiveMessages(${key}) length mismatch`);
    foreach int i in 0 ..< actual.length() {
        assertChatMessageEquals(actual[i], expected[i]);
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
    test:assertFail("ChatMessage type mismatch");
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
    test:assertFail("Message content type mismatch");
}
