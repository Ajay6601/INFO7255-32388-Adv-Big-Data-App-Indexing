const express = require("express");
const bodyParser = require("body-parser");
const crypto = require("crypto");
const { v4: uuidv4 } = require("uuid");
const Ajv = require("ajv");
const Redis = require("ioredis");
const jwt = require("jsonwebtoken");
const { OAuth2Client } = require("google-auth-library");
const { Client } = require("@elastic/elasticsearch");
const amqp = require("amqplib");
const { createHash } = require("crypto");

// Initialize Express App
const app = express();
const port = 3000;
app.use(bodyParser.json());

// Initialize Redis Client (Key/Value Store)
const redis = new Redis({
    host: "localhost",
    port: 6379,
});

// Initialize Elasticsearch Client
const esClient = new Client({ node: "http://localhost:9200" });

// Elasticsearch service constants
const INDEX_NAME = "plans_v2";

// Elasticsearch Service Functions
let MapOfDocuments = {};
let listOfKeys = [];


const convertMapToDocumentIndex = async (jsonObject, parentId, objectName, parentObjId) => {
    const valueMap = {};
    const map = {};

    for (const [key, value] of Object.entries(jsonObject)) {
        const redisKey = `${jsonObject.objectType}:${parentId}`;
        if (Array.isArray(value)) {
            await convertToList(value, jsonObject.objectId, key, parentObjId);
        } else if (typeof value === 'object' && value !== null) {
            await convertMapToDocumentIndex(value, jsonObject.objectId, key, parentObjId);
        } else {
            valueMap[key] = value;
            map[redisKey] = valueMap;
        }
    }

    if (objectName === "plan") {
        valueMap["join_field"] = "plan";
    } else if (objectName === "planCostShares") {
        valueMap["join_field"] = {
            "name": "plancostshare",
            "parent": parentId
        };
    } else if (objectName === "linkedService") {
        // Skip creating a separate document for linkedService
        // Instead, include it as a nested object in the parent
        return map;
    } else if (objectName.match(/^-?\d+$/)) {
        parentId = parentObjId;
        valueMap["join_field"] = {
            "name": "linkedPlanService",
            "parent": parentObjId
        };
    } else {
        // For other objects, check if they should be indexed separately
        const validJoinNames = ["plancostshare", "linkedPlanService", "childOfLinkedPlanService"];
        const normalizedName = objectName.toLowerCase();
        
        if (validJoinNames.includes(normalizedName)) {
            valueMap["join_field"] = {
                "name": normalizedName,
                "parent": parentId
            };
        } else {
            // Skip objects that don't match the known join types
            console.log(`Skipping object with unknown join name: ${objectName}`);
            return map;
        }
    }

    const id = `${parentId}:${jsonObject.objectId}`;
    if (!!jsonObject?.objectId) MapOfDocuments[id] = valueMap;
    return map;
};


const convertToList = async (jsonArray, parentId, objectName, parentObjId) => {
    const list = [];
    for (let i = 0; i < jsonArray.length; i++) {
        let value = jsonArray[i];
        if (Array.isArray(value)) {
            value = await convertToList(value, parentId, objectName, parentObjId);
        } else if (typeof value === 'object' && value !== null) {
            value = await convertMapToDocumentIndex(value, parentId, objectName, parentObjId);
        }
        list.push(value);
    }
    return list;
};



const convertToKeysList = async (jsonArray) => {
    let list = [];
    for (let value of jsonArray) {
        if (Array.isArray(value)) {
            value = await convertToKeysList(value);
        } else if (typeof value === 'object' && value !== null) {
            value = await convertToKeys(value);
        }
        list.push(value);
    }
    return list;
};

const convertToKeys = async (jsonObject) => {
    const map = {};
    const valueMap = {};

    for (const [key, value] of Object.entries(jsonObject)) {
        const redisKey = jsonObject["objectId"];
        if (Array.isArray(value)) {
            await convertToKeysList(value);
        } else if (typeof value === 'object' && value !== null) {
            await convertToKeys(value);
        } else {
            valueMap[key] = value;
            map[redisKey] = valueMap;
        }
    }

    listOfKeys.push(jsonObject["objectId"]);
    return map;
};


const postDocument = async (plan) => {
    try {
        MapOfDocuments = {};
        
        // Add these debug logs
        console.log("Before processing plan:", plan.objectId);
        console.log("LinkedPlanServices count:", plan.linkedPlanServices?.length || 0);
        
        await convertMapToDocumentIndex(plan, "", "plan", plan.objectId);
        
        console.log("After processing, documents count:", Object.keys(MapOfDocuments).length);
        console.log("Document keys:", Object.keys(MapOfDocuments));
        // End of added logs
        
        console.log(`Indexing ${Object.keys(MapOfDocuments).length} documents`);
        
        for (const [key, value] of Object.entries(MapOfDocuments)) {
            const [parentId, objectId] = key.split(":");
            console.log(`Indexing document: ID=${objectId}, parent=${parentId || 'none'}, join_field=`, 
                         JSON.stringify(value.join_field));
            
            await esClient.index({
                index: INDEX_NAME,
                id: objectId,
                routing: parentId || undefined,
                body: value,
            });
        }
        
        return { message: 'Document has been posted', status: 200 };
    } catch (e) {
        console.error("Error during indexing:", e);
        if (e.meta && e.meta.body) {
            console.error("Elasticsearch error details:", JSON.stringify(e.meta.body, null, 2));
        }
        return { message: 'Document has not been posted', status: 500 };
    }
};

// const postDocument = async (plan) => {
//     try {
//         MapOfDocuments = {};
//         await convertMapToDocumentIndex(plan, "", "plan", plan.objectId);
//         for (const [key, value] of Object.entries(MapOfDocuments)) {
//             const [parentId, objectId] = key.split(":");
//             await esClient.index({
//                 index: INDEX_NAME,
//                 id: objectId,
//                 routing: parentId || undefined,
//                 body: value,
//             });
//         }
//         return { message: 'Document has been posted', status: 200 };
//     } catch (e) {
//         console.log("Error", e);
//         return { message: 'Document has not been posted', status: 500 };
//     }
// };

// const postDocument = async (plan) => {
//     try {
//         MapOfDocuments = {};
//         await convertMapToDocumentIndex(plan, "", "plan", plan.objectId);
        
//         console.log(`Indexing ${Object.keys(MapOfDocuments).length} documents`);
        
//         for (const [key, value] of Object.entries(MapOfDocuments)) {
//             const [parentId, objectId] = key.split(":");
//             console.log(`Indexing document: ID=${objectId}, parent=${parentId || 'none'}, join_field=`, 
//                          JSON.stringify(value.join_field));
            
//             await client.index({
//                 index: INDEX_NAME,
//                 id: objectId,
//                 routing: parentId || undefined,
//                 body: value,
//             });
//         }
        
//         return { message: 'Document has been posted', status: 200 };
//     } catch (e) {
//         console.error("Error during indexing:", e);
//         if (e.meta && e.meta.body) {
//             console.error("Elasticsearch error details:", JSON.stringify(e.meta.body, null, 2));
//         }
//         return { message: 'Document has not been posted', status: 500 };
//     }
// };

const deleteDocument = async (jsonObject) => {
    listOfKeys = [];
    await convertToKeys(jsonObject);
    console.log("Deleting document keys:", listOfKeys);
    
    for (const key of listOfKeys) {
        try {
            await esClient.delete({
                index: INDEX_NAME,
                id: key,
            });
            console.log(`Index ${key} has been deleted!`);
        } catch (err) {
            console.error(`Error deleting index ${key}:`, err.message);
        }
    }
    
    return { message: 'Document deletion completed', status: 200 };
};

// Initialize Elasticsearch index for parent-child support
async function initializeIndex() {
    try {
        const indexExists = await esClient.indices.exists({ index: INDEX_NAME });
        if (!indexExists.body) {
            console.log(`Creating new '${INDEX_NAME}' index with join mapping...`);
            await esClient.indices.create({
                index: INDEX_NAME,
                body: {
                    mappings: {
                        properties: {
                            join_field: {
                                type: "join",
                                relations: {
                                    plan: ["linkedPlanService", "plancostshare"],
                                    linkedPlanService: ["childOfLinkedPlanService"]
                                }
                            }
                        }
                    }
                }
            });
            console.log(`Created '${INDEX_NAME}' index with parent-child mapping.`);
        } else {
            console.log(`'${INDEX_NAME}' index already exists.`);
        }
    } catch (err) {
        if (
            err.meta &&
            err.meta.body &&
            err.meta.body.error &&
            err.meta.body.error.type === "resource_already_exists_exception"
        ) {
            console.warn(`Tried to create '${INDEX_NAME}' index, but it already exists.`);
        } else {
            console.error(`Error creating '${INDEX_NAME}' index:`, err);
        }
    }
}

// JSON Schema for Validation
const schema = {
    type: "object",
    properties: {
        planCostShares: {
            type: "object",
            properties: {
                deductible: { type: "number" },
                _org: { type: "string" },
                copay: { type: "number" },
                objectId: { type: "string" },
                objectType: { type: "string" },
            },
            required: ["deductible", "_org", "copay", "objectId", "objectType"],
        },
        linkedPlanServices: {
            type: "array",
            items: {
                type: "object",
                properties: {
                    linkedService: {
                        type: "object",
                        properties: {
                            _org: { type: "string" },
                            objectId: { type: "string" },
                            objectType: { type: "string" },
                            name: { type: "string" },
                        },
                        required: ["_org", "objectId", "objectType", "name"],
                    },
                    planserviceCostShares: {
                        type: "object",
                        properties: {
                            deductible: { type: "number" },
                            _org: { type: "string" },
                            copay: { type: "number" },
                            objectId: { type: "string" },
                            objectType: { type: "string" },
                        },
                        required: ["deductible", "_org", "copay", "objectId", "objectType"],
                    },
                    _org: { type: "string" },
                    objectId: { type: "string" },
                    objectType: { type: "string" },
                },
                required: ["linkedService", "planserviceCostShares", "_org", "objectId", "objectType"],
            },
        },
        _org: { type: "string" },
        objectId: { type: "string" },
        objectType: { type: "string" },
        planType: { type: "string" },
        creationDate: { type: "string" },
    },
    required: ["planCostShares", "linkedPlanServices", "_org", "objectId", "objectType", "planType", "creationDate"],
};

// Initialize AJV
const ajv = new Ajv();
const validate = ajv.compile(schema);

// Generate ETag
const generateEtag = (data) => {
    return crypto.createHash("md5").update(JSON.stringify(data)).digest("hex");
};

// Initialize RabbitMQ Connection
let channel;
async function setupQueue() {
    try {
        const connection = await amqp.connect("amqp://localhost");
        channel = await connection.createChannel();
        await channel.assertQueue("indexingQueue", { durable: true });

        console.log("RabbitMQ connected. Queue 'indexingQueue' is ready.");

        // Start consuming messages only after the queue is ready
        consumeQueue();
    } catch (error) {
        console.error("RabbitMQ Connection Error:", error);
    }
}

// RabbitMQ Consumer: Index Data into Elasticsearch
async function consumeQueue() {
    if (!channel) {
        console.error("RabbitMQ channel not initialized.");
        return;
    }

    channel.consume("indexingQueue", async (msg) => {
        if (msg !== null) {
            const data = JSON.parse(msg.content.toString());
            
            try {
                // Use the elastic service function to index the document
                const result = await postDocument(data);
                console.log("Indexing result:", result);
            } catch (error) {
                console.error("Error indexing document:", error);
            }
            
            channel.ack(msg);
        }
    });
}

// Google OAuth2 Authentication
const client = new OAuth2Client(
    "203784104200-4k5d6qa4cilha0500i83a09bqmj8dbf3.apps.googleusercontent.com"
);

// Middleware for OAuth Token Verification
const verifyGoogleToken = async (req, res, next) => {
    // Extract the token from the Authorization header
    const token = req.header("Authorization")?.replace("Bearer ", "");
    if (!token) {
        return res.status(403).json({ error: "Authorization token missing" });
    }

    try {
        // Verify the ID token using Google's OAuth2Client
        const ticket = await client.verifyIdToken({
            idToken: token,
            audience: '203784104200-4k5d6qa4cilha0500i83a09bqmj8dbf3.apps.googleusercontent.com',
        });

        // Get the payload (user info) from the verified ID token
        const payload = ticket.getPayload();

        // Attach the payload (user info) to the request object
        req.user = payload;

        // Continue to the next middleware/route handler
        next();
    } catch (error) {
        return res.status(500).json({
            error: "Token verification failed",
            details: error.message,
        });
    }
};

// CREATE: Store Data in Redis & Index in Elasticsearch
app.post("/v1/plan", verifyGoogleToken, async (req, res) => {
    const data = req.body;
    if (!validate(data)) {
        return res.status(400).json({ error: "Validation failed", details: validate.errors });
    }

    const { objectId } = data;

    const existingPlan = await redis.get(objectId);
    if (existingPlan) {
        return res.status(409).json({
            message: "Conflict - Plan already exists",
            status: "failed"
        });
    }

    const etag = generateEtag(data);
    data.etag = etag;

    // Store in Redis (Key: objectId, Value: JSON String)
    await redis.set(data.objectId, JSON.stringify(data));

    // Queue indexing operation
    channel.sendToQueue("indexingQueue", Buffer.from(JSON.stringify(data)));

    res.set({
        "X-Powered-By": "Express",
        "Etag": etag,
        "Content-Type": "application/json",
    });

    return res.status(201).json({ id: data.objectId, message: "Plan created successfully" });
});

// UPDATE: Partial Update with PATCH
app.patch("/v1/plan/:id", verifyGoogleToken, async (req, res) => {
    const planId = req.params.id;
    const data = req.body;

    const clientEtag = req.header("If-Match");
    if (!clientEtag) {
        return res.status(428).json({
            message: "Precondition Required - If-Match header is missing",
            status: "failed"
        });
    }

    const planData = await redis.get(planId);
    if (!planData) {
        return res.status(404).json({ message: "Plan not found" });
    }

    let plan = JSON.parse(planData);

    if (clientEtag !== plan.etag) {
        return res.status(412).json({
            message: "Precondition Failed - ETag does not match",
            status: "failed"
        });
    }

    Object.keys(data).forEach((key) => {
        if (Array.isArray(data[key]) && Array.isArray(plan[key])) {
            data[key].forEach((updatedItem) => {
                const index = plan[key].findIndex(item => item.objectId === updatedItem.objectId);
                if (index !== -1) {
                    plan[key][index] = { ...plan[key][index], ...updatedItem };
                } else {
                    plan[key].push(updatedItem);
                }
            });
        } else {
            plan[key] = data[key];
        }
    });

    if (!validate(plan)) {
        return res.status(400).json({
            message: "Invalid JSON format",
            errors: validate.errors
        });
    }

    const newEtag = createHash("sha256").update(JSON.stringify(plan)).digest("base64");

    if (newEtag === plan.etag) {
        return res.status(304).json({
            message: "Plan is already up to date",
            status: "not_modified"
        });
    }

    plan.etag = newEtag;

    await redis.set(planId, JSON.stringify(plan));

    // Queue indexing operation to update Elasticsearch
    channel.sendToQueue("indexingQueue", Buffer.from(JSON.stringify(plan)));

    res.set({
        "X-Powered-By": "Express",
        "Etag": newEtag,
        "Content-Type": "application/json",
    });

    return res.status(200).json(plan);
});

// GET: Retrieve Data from Redis
app.get("/v1/plan/:id", verifyGoogleToken, async (req, res) => {
    try {
        const planData = await redis.get(req.params.id);

        if (!planData) {
            return res.status(404).json({ error: "Plan not found" });
        }

        const plan = JSON.parse(planData);
        const clientEtag = req.header("If-None-Match");

        // If client's ETag matches the current ETag, return 304 Not Modified
        if (clientEtag && clientEtag === plan.etag) {
            return res.status(304).end();
        }

        // Otherwise, return the updated plan with ETag
        res.set({
            "X-Powered-By": "Express",
            "Etag": plan.etag,
            "Content-Type": "application/json",
        });

        return res.status(200).json(plan);
    } catch (err) {
        console.error("Error retrieving plan:", err);
        return res.status(500).json({ error: "Internal Server Error" });
    }
});

// DELETE: Remove Data & Perform Cascading Deletes
app.delete("/v1/plan/:id", verifyGoogleToken, async (req, res) => {
    const planId = req.params.id;
    const planData = await redis.get(planId);

    if (!planData) {
        return res.status(404).json({ error: "Plan not found" });
    }

    let plan = JSON.parse(planData);

    // Cascading delete: Remove child records
    if (plan.linkedPlanServices) {
        for (const service of plan.linkedPlanServices) {
            await redis.del(service.objectId);
        }
    }

    await redis.del(planId);
    
    // Use our Elasticsearch service to delete the document and its children
    await deleteDocument(plan);

    res.status(204).end();
});

// SEARCH: Query Data in Elasticsearch
app.get("/v1/search", verifyGoogleToken, async (req, res) => {
    const query = req.query.q;

    const result = await esClient.search({
        index: INDEX_NAME,
        body: {
            query: {
                match: { planType: query }
            }
        }
    });

    res.json(result.hits.hits.map(hit => hit._source));
});

// Add a new endpoint for advanced Elasticsearch queries
app.post("/v1/plans/_search", verifyGoogleToken, async (req, res) => {
    try {
        const searchQuery = req.body;
        const result = await esClient.search({
            index: INDEX_NAME,
            body: searchQuery
        });
        
        res.json(result);
    } catch (error) {
        console.error("Search Error:", error);
        res.status(400).json({
            message: "Error executing search query",
            error: error.message
        });
    }
});

// Add specific endpoints for parent-child queries
app.get("/v1/plans/query/children/:parentId", verifyGoogleToken, async (req, res) => {
    try {
        const parentId = req.params.parentId;
        const result = await esClient.search({
            index: INDEX_NAME,
            body: {
                query: {
                    has_parent: {
                        parent_type: "plan",
                        query: {
                            term: {
                                "_id": parentId
                            }
                        }
                    }
                }
            }
        });
        
        res.json(result);
    } catch (error) {
        console.error("Search Error:", error);
        res.status(400).json({
            message: "Error executing parent-child query",
            error: error.message
        });
    }
});

app.get("/v1/plans/query/service-children/:serviceId", verifyGoogleToken, async (req, res) => {
    try {
        const serviceId = req.params.serviceId;
        const result = await esClient.search({
            index: INDEX_NAME,
            body: {
                query: {
                    has_parent: {
                        parent_type: "linkedPlanService",
                        query: {
                            term: {
                                "_id": serviceId
                            }
                        }
                    }
                }
            }
        });
        
        res.json(result);
    } catch (error) {
        console.error("Search Error:", error);
        res.status(400).json({
            message: "Error executing parent-child query",
            error: error.message
        });
    }
});

app.get("/v1/plans/query/plan-with-copay/:minCopay", verifyGoogleToken, async (req, res) => {
    try {
        const minCopay = parseInt(req.params.minCopay);
        const result = await esClient.search({
            index: INDEX_NAME,
            body: {
                query: {
                    bool: {
                        must: [
                            { term: { "join_field": "plan" }},
                            { range: { "planCostShares.copay": { "gte": minCopay }}}
                        ]
                    }
                }
            }
        });
        
        res.json(result);
    } catch (error) {
        console.error("Search Error:", error);
        res.status(400).json({
            message: "Error executing query",
            error: error.message
        });
    }
});

app.get("/v1/plans/query/services-by-plan/:planId", verifyGoogleToken, async (req, res) => {
    try {
        const planId = req.params.planId;
        const result = await esClient.search({
            index: INDEX_NAME,
            body: {
                query: {
                    bool: {
                        must: [
                            {
                                term: {
                                    "objectType": "planservice"
                                }
                            },
                            {
                                has_parent: {
                                    parent_type: "plan",
                                    query: {
                                        term: {
                                            "_id": planId
                                        }
                                    }
                                }
                            }
                        ]
                    }
                }
            }
        });
        
        res.json(result);
    } catch (error) {
        console.error("Search Error:", error);
        res.status(400).json({
            message: "Error executing query",
            error: error.message
        });
    }
});

// Initialize Elasticsearch index for parent-child support
initializeIndex().catch(console.error);

// Ensure the queue is set up before the server starts
setupQueue();

// Start the Express server
app.listen(port, () => {
    console.log(`Server running at http://localhost:${port}`);
});