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

import ballerina/lang.regexp;
import ballerinax/aws.dynamodb;

// In-memory storage shared between `FakeDynamoDbClient` (which exposes the
// subset of the `dynamodb:Client` surface the store consumes) and the tests
// themselves (which use the `peek*` helpers to confirm what was actually
// persisted). The split is necessary because `test:mock(dynamodb:Client, ...)`
// only accepts a mock object whose methods exist on `dynamodb:Client` — adding
// inspection helpers directly to the mock would fail mock validation.

const string COMPOSITE_KEY_DELIM = "\u{1}";

isolated class FakeStorage {
    private map<()> tables = {};
    // Composite key (`tableName + DELIM + partition + DELIM + sort`) -> body string.
    private map<string> items = {};
    // Counter key (`tableName + DELIM + partition`) -> last issued sequence value.
    private map<int> counters = {};

    // ----- Fault-injection knobs (configured by tests before exercising the
    // store). Both default to 0, so unless a test opts in the fake behaves like
    // an always-healthy DynamoDB and existing tests are unaffected. -----

    // While > 0, the next `writeBatchItems` call persists nothing and reports
    // every request back as `UnprocessedItems`, then decrements. This drives the
    // store's BatchWriteItem retry/backoff loop (`writeChunk`).
    private int unprocessedRoundsRemaining = 0;
    // While > 0, the next `describeTable` against an existing table reports
    // `CREATING` instead of `ACTIVE`, then decrements. This drives the store's
    // `waitForTableActive` polling loop after a table is created.
    private int activationPollsRemaining = 0;

    isolated function init() {
    }

    // Arms `rounds` consecutive `writeBatchItems` calls to report all of their
    // items as unprocessed (without persisting them).
    isolated function setUnprocessedRounds(int rounds) {
        lock {
            self.unprocessedRoundsRemaining = rounds;
        }
    }

    // Returns true (and consumes one round) if the current write should be
    // faulted into an all-unprocessed response.
    isolated function consumeUnprocessedRound() returns boolean {
        lock {
            if self.unprocessedRoundsRemaining <= 0 {
                return false;
            }
            self.unprocessedRoundsRemaining -= 1;
            return true;
        }
    }

    // Arms `polls` consecutive `describeTable` calls (against an existing table)
    // to report `CREATING` before the table is reported `ACTIVE`.
    isolated function setActivationPolls(int polls) {
        lock {
            self.activationPollsRemaining = polls;
        }
    }

    // Returns true (and consumes one poll) if the table should still report
    // `CREATING` on this `describeTable` call.
    isolated function consumeActivationPoll() returns boolean {
        lock {
            if self.activationPollsRemaining <= 0 {
                return false;
            }
            self.activationPollsRemaining -= 1;
            return true;
        }
    }

    isolated function tableExists(string tableName) returns boolean {
        lock {
            return self.tables.hasKey(tableName);
        }
    }

    // Returns true if the table was newly created, false if it already existed.
    isolated function createTableIfAbsent(string tableName) returns boolean {
        lock {
            if self.tables.hasKey(tableName) {
                return false;
            }
            self.tables[tableName] = ();
            return true;
        }
    }

    isolated function getItemBody(string tableName, string pk, string sk) returns string? {
        lock {
            return self.items[compositeItemKey(tableName, pk, sk)];
        }
    }

    isolated function putItem(string tableName, string pk, string sk, string body) {
        lock {
            self.items[compositeItemKey(tableName, pk, sk)] = body;
        }
    }

    isolated function removeItem(string tableName, string pk, string sk) {
        string ckey = compositeItemKey(tableName, pk, sk);
        string counterKeyValue = counterKey(tableName, pk);
        lock {
            if self.items.hasKey(ckey) {
                _ = self.items.remove(ckey);
            }
            if sk == COUNTER_MESSAGE_ID && self.counters.hasKey(counterKeyValue) {
                _ = self.counters.remove(counterKeyValue);
            }
        }
    }

    // Atomically increments the per-partition counter by `delta` and returns the new value.
    isolated function incrementCounter(string tableName, string pk, string sk, int delta) returns int {
        string counterKeyValue = counterKey(tableName, pk);
        string ckey = compositeItemKey(tableName, pk, sk);
        lock {
            int current = self.counters[counterKeyValue] ?: 0;
            int next = current + delta;
            self.counters[counterKeyValue] = next;
            // Reflect the counter into the items map so a partition-wide scan
            // (e.g. `removeAll`) sees and clears it the same way the real
            // DynamoDB would.
            self.items[ckey] = next.toString();
            return next;
        }
    }

    // Returns the sort ids under (tableName, pk) optionally restricted to a
    // sort-key prefix. The result is sorted ascending and snapshot-cloned.
    isolated function querySortIds(string tableName, string pk, string? prefixFilter)
            returns string[] {
        string partitionPrefixValue = partitionPrefix(tableName, pk);
        lock {
            string[] matched = [];
            foreach string compositeKey in self.items.keys() {
                if !compositeKey.startsWith(partitionPrefixValue) {
                    continue;
                }
                string sortId = compositeKey.substring(partitionPrefixValue.length());
                if prefixFilter is string && !sortId.startsWith(prefixFilter) {
                    continue;
                }
                matched.push(sortId);
            }
            return matched.sort().clone();
        }
    }

    // ----- Test inspection helpers (not part of dynamodb:Client surface). -----

    isolated function peekBody(string tableName, string pk, string sk) returns string? {
        lock {
            return self.items[compositeItemKey(tableName, pk, sk)];
        }
    }

    isolated function peekSortIds(string tableName, string pk) returns string[] {
        string prefix = partitionPrefix(tableName, pk);
        lock {
            string[] sortIds = [];
            foreach string compositeKey in self.items.keys() {
                if compositeKey.startsWith(prefix) {
                    sortIds.push(compositeKey.substring(prefix.length()));
                }
            }
            return sortIds.sort().clone();
        }
    }

    isolated function peekCounter(string tableName, string pk) returns int {
        lock {
            return self.counters[counterKey(tableName, pk)] ?: 0;
        }
    }

    isolated function hasTable(string tableName) returns boolean {
        lock {
            return self.tables.hasKey(tableName);
        }
    }
}

// The single "current" FakeStorage instance that every `FakeDynamoDbClient`
// call routes through. `newFakeStorage()` rotates it for each test so state
// never leaks across tests.
//
// We use module-level state here because `test:mock(dynamodb:Client, mockObj)`
// validates that the mock object has no extra fields or methods beyond what
// `dynamodb:Client` exposes. Holding the storage in a field on the mock object
// itself would fail that validation.
isolated FakeStorage activeStorage = new ();

isolated function newFakeStorage() returns FakeStorage {
    FakeStorage fresh = new ();
    lock {
        activeStorage = fresh;
    }
    return fresh;
}

isolated function getActiveStorage() returns FakeStorage {
    lock {
        return activeStorage;
    }
}

// An in-memory fake of `dynamodb:Client` that implements only the subset of
// remote operations the `ShortTermMemoryStore` calls into. It is installed via
// `test:mock(dynamodb:Client, new FakeDynamoDbClient())`.
//
// The fake intentionally mirrors a few real DynamoDB behaviours that the store
// depends on:
//   * `describeTable` returns a `ResourceNotFoundException`-flavoured error
//     when the table has not been created, and an `ACTIVE` status once it
//     exists.
//   * `createTable` errors with `ResourceInUseException` if invoked twice for
//     the same name (so the second store on the same fake reuses the existing
//     table).
//   * `query` honours `ScanIndexForward` and the two `KeyConditionExpression`
//     shapes the store issues (`#pk = :pk` and
//     `#pk = :pk and begins_with(#sk, :prefix)`), plus the `#sk`-only
//     `ProjectionExpression` used by counting and sort-id lookups.
//   * `updateItem` understands the `ADD #seq :delta` expression and returns
//     the new counter value under `Attributes`.
//   * `writeBatchItems` processes `PutRequest` and `DeleteRequest` entries one
//     at a time and never reports unprocessed items.
//
// The class itself carries no fields — all state lives in `activeStorage`.
isolated client class FakeDynamoDbClient {

    isolated function init() {
    }

    remote isolated function describeTable(string tableName) returns dynamodb:TableDescription|error {
        FakeStorage storage = getActiveStorage();
        if !storage.tableExists(tableName) {
            return error(string `ResourceNotFoundException: table '${tableName}' not found`);
        }
        // When a test has armed activation polls, report the table as still
        // creating so the store keeps polling in `waitForTableActive`.
        if storage.consumeActivationPoll() {
            return {TableName: tableName, TableStatus: dynamodb:CREATING};
        }
        return {TableName: tableName, TableStatus: dynamodb:ACTIVE};
    }

    remote isolated function createTable(dynamodb:TableCreateInput input)
            returns dynamodb:TableDescription|error {
        FakeStorage storage = getActiveStorage();
        if !storage.createTableIfAbsent(input.TableName) {
            return error(string `ResourceInUseException: table '${input.TableName}' already exists`);
        }
        return {TableName: input.TableName, TableStatus: dynamodb:ACTIVE};
    }

    remote isolated function getItem(dynamodb:ItemGetInput input)
            returns dynamodb:ItemGetOutput|error {
        FakeStorage storage = getActiveStorage();
        if !storage.tableExists(input.TableName) {
            return error(string `ResourceNotFoundException: table '${input.TableName}'`);
        }
        [string, string] [pk, sk] = check extractCompositeKey(input.Key);
        string? body = storage.getItemBody(input.TableName, pk, sk);
        if body is () {
            return {};
        }
        map<dynamodb:AttributeValue> item = {
            [PARTITION_KEY_ATTRIBUTE]: {S: pk},
            [SORT_KEY_ATTRIBUTE]: {S: sk},
            [BODY_ATTRIBUTE]: {S: body}
        };
        return {Item: item};
    }

    remote isolated function createItem(dynamodb:ItemCreateInput input)
            returns dynamodb:ItemDescription|error {
        FakeStorage storage = getActiveStorage();
        if !storage.tableExists(input.TableName) {
            return error(string `ResourceNotFoundException: table '${input.TableName}'`);
        }
        [string, string] [pk, sk] = check extractCompositeKey(input.Item);
        dynamodb:AttributeValue? bodyAttr = input.Item[BODY_ATTRIBUTE];
        string? body = bodyAttr is () ? () : bodyAttr?.S;
        if body is () {
            return error("ValidationException: missing Body attribute on PutItem");
        }
        storage.putItem(input.TableName, pk, sk, body);
        return {};
    }

    remote isolated function updateItem(dynamodb:ItemUpdateInput input)
            returns dynamodb:ItemDescription|error {
        FakeStorage storage = getActiveStorage();
        if !storage.tableExists(input.TableName) {
            return error(string `ResourceNotFoundException: table '${input.TableName}'`);
        }
        [string, string] [pk, sk] = check extractCompositeKey(input.Key);
        string expr = (input.UpdateExpression ?: "").trim();
        if !regexp:isFullMatch(re `ADD\s+#seq\s+:delta`, expr) {
            return error("FakeDynamoDbClient supports only the `ADD #seq :delta` UpdateExpression");
        }
        map<dynamodb:AttributeValue>? exprValues = input?.ExpressionAttributeValues;
        if exprValues is () {
            return error("ValidationException: missing ExpressionAttributeValues");
        }
        dynamodb:AttributeValue? deltaAttr = exprValues[":delta"];
        string? deltaStr = deltaAttr is () ? () : deltaAttr?.N;
        if deltaStr is () {
            return error("ValidationException: missing :delta value");
        }
        int|error delta = int:fromString(deltaStr);
        if delta is error {
            return error("ValidationException: invalid :delta value", delta);
        }

        int newSeq = storage.incrementCounter(input.TableName, pk, sk, delta);
        return {Attributes: {[SEQUENCE_ATTRIBUTE]: {N: newSeq.toString()}}};
    }

    remote isolated function deleteItem(dynamodb:ItemDeleteInput input)
            returns dynamodb:ItemDescription|error {
        FakeStorage storage = getActiveStorage();
        if !storage.tableExists(input.TableName) {
            return error(string `ResourceNotFoundException: table '${input.TableName}'`);
        }
        [string, string] [pk, sk] = check extractCompositeKey(input.Key);
        storage.removeItem(input.TableName, pk, sk);
        return {};
    }

    remote isolated function query(dynamodb:QueryInput input)
            returns stream<dynamodb:QueryOutput, error?>|error {
        FakeStorage storage = getActiveStorage();
        if !storage.tableExists(input.TableName) {
            return error(string `ResourceNotFoundException: table '${input.TableName}'`);
        }
        string expr = (input.KeyConditionExpression ?: "").trim();
        map<dynamodb:AttributeValue>? exprValues = input?.ExpressionAttributeValues;
        if exprValues is () {
            return error("ValidationException: missing ExpressionAttributeValues");
        }
        dynamodb:AttributeValue? pkAttr = exprValues[":pk"];
        string? pkValue = pkAttr is () ? () : pkAttr?.S;
        if pkValue is () {
            return error("ValidationException: :pk missing or wrong type");
        }
        string? prefixFilter = ();
        if expr.includes("begins_with") {
            dynamodb:AttributeValue? prefAttr = exprValues[":prefix"];
            string? prefValue = prefAttr is () ? () : prefAttr?.S;
            if prefValue is () {
                return error("ValidationException: :prefix missing or wrong type");
            }
            prefixFilter = prefValue;
        }

        boolean forward = input?.ScanIndexForward ?: true;
        string projection = input?.ProjectionExpression ?: "";

        string[] sortIds = storage.querySortIds(input.TableName, pkValue, prefixFilter);
        if !forward {
            sortIds = sortIds.reverse();
        }

        dynamodb:QueryOutput[] outputs = [];
        foreach string sortId in sortIds {
            map<dynamodb:AttributeValue> item;
            if projection == "#sk" {
                item = {[SORT_KEY_ATTRIBUTE]: {S: sortId}};
            } else {
                string body = storage.getItemBody(input.TableName, pkValue, sortId) ?: "";
                item = {
                    [PARTITION_KEY_ATTRIBUTE]: {S: pkValue},
                    [SORT_KEY_ATTRIBUTE]: {S: sortId},
                    [BODY_ATTRIBUTE]: {S: body}
                };
            }
            outputs.push({Item: item});
        }
        QueryOutputIterator iterator = new (outputs);
        return new stream<dynamodb:QueryOutput, error?>(iterator);
    }

    remote isolated function writeBatchItems(dynamodb:BatchItemInsertInput input)
            returns dynamodb:BatchItemInsertOutput|error {
        FakeStorage storage = getActiveStorage();
        // Fault injection: while a test has armed unprocessed rounds, persist
        // nothing and echo every request straight back as `UnprocessedItems`, so
        // the store must retry the same chunk. Real DynamoDB does this under
        // throttling; the store's `writeChunk` retry loop is what we exercise.
        if storage.consumeUnprocessedRound() {
            return {UnprocessedItems: input.RequestItems.clone()};
        }
        foreach [string, dynamodb:WriteRequest[]] [tableName, requests] in input.RequestItems.entries() {
            if !storage.tableExists(tableName) {
                return error(string `ResourceNotFoundException: table '${tableName}'`);
            }
            foreach dynamodb:WriteRequest request in requests {
                dynamodb:PutRequest? putRequest = request?.PutRequest;
                dynamodb:DeleteRequest? deleteRequest = request?.DeleteRequest;
                if putRequest is dynamodb:PutRequest {
                    [string, string] [pk, sk] = check extractCompositeKey(putRequest.Item);
                    dynamodb:AttributeValue? bodyAttr = putRequest.Item[BODY_ATTRIBUTE];
                    string? body = bodyAttr is () ? () : bodyAttr?.S;
                    if body is () {
                        return error("ValidationException: missing Body attribute in PutRequest");
                    }
                    storage.putItem(tableName, pk, sk, body);
                } else if deleteRequest is dynamodb:DeleteRequest {
                    [string, string] [pk, sk] = check extractCompositeKey(deleteRequest.Key);
                    storage.removeItem(tableName, pk, sk);
                }
            }
        }
        return {};
    }
}

// Iterator object backing the stream returned by `FakeDynamoDbClient.query`.
class QueryOutputIterator {
    private final dynamodb:QueryOutput[] items;
    private int index = 0;

    isolated function init(dynamodb:QueryOutput[] items) {
        self.items = items;
    }

    public isolated function next() returns record {|dynamodb:QueryOutput value;|}|error? {
        if self.index >= self.items.length() {
            return ();
        }
        dynamodb:QueryOutput current = self.items[self.index];
        self.index += 1;
        return {value: current};
    }
}

isolated function compositeItemKey(string tableName, string pk, string sk) returns string =>
    tableName + COMPOSITE_KEY_DELIM + pk + COMPOSITE_KEY_DELIM + sk;

isolated function counterKey(string tableName, string pk) returns string =>
    tableName + COMPOSITE_KEY_DELIM + pk;

isolated function partitionPrefix(string tableName, string pk) returns string =>
    tableName + COMPOSITE_KEY_DELIM + pk + COMPOSITE_KEY_DELIM;

isolated function extractCompositeKey(map<dynamodb:AttributeValue> attrs) returns [string, string]|error {
    dynamodb:AttributeValue? pkAttr = attrs[PARTITION_KEY_ATTRIBUTE];
    dynamodb:AttributeValue? skAttr = attrs[SORT_KEY_ATTRIBUTE];
    string? pkValue = pkAttr is () ? () : pkAttr?.S;
    string? skValue = skAttr is () ? () : skAttr?.S;
    if pkValue is () || skValue is () {
        return error("ValidationException: missing partition or sort key");
    }
    return [pkValue, skValue];
}
