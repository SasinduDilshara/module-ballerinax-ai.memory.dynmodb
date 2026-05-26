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
import ballerina/io;
import ballerinax/ai.memory.dynamodb;
import ballerinax/ai.openai;

configurable string accessKeyId = ?;
configurable string secretAccessKey = ?;
configurable string region = ?;
configurable string openAiApiKey = ?;
configurable string sessionId = "demo-session";

final ai:ShortTermMemoryStore store = check new dynamodb:ShortTermMemoryStore({
    awsCredentials: {accessKeyId, secretAccessKey},
    region
});

final ai:Memory memory = check new ai:ShortTermMemory(store);

final ai:ModelProvider openAiModel = check new openai:ModelProvider(openAiApiKey, modelType = openai:GPT_4O);

final ai:Agent chatAgent = check new (
    systemPrompt = {
        role: "Friendly Assistant",
        instructions: "You are a friendly assistant. Reply in one short sentence."
    },
    model = openAiModel,
    memory = memory,
    verbose = false
);

public function main() returns error? {
    string[] queries = [
        "Hi! My name is Alice and I love hiking.",
        "What's my name and one hobby I enjoy?"
    ];

    foreach string query in queries {
        io:println(string `User: ${query}`);
        string response = check chatAgent.run(query, sessionId);
        io:println(string `Agent: ${response}`);
        io:println();
    }
}
