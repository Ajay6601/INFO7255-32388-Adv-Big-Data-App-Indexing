const express = require("express");
const bodyParser = require("body-parser");
const crypto = require("crypto");
const { v4: uuidv4 } = require("uuid");
const Ajv = require("ajv");
const Redis = require("ioredis");
const jwt = require("jsonwebtoken");
const { OAuth2Client } = require("google-auth-library");
const { postDocument, initializeIndex } = require('./elasticsearchService');
const { Client } = require("@elastic/elasticsearch");
const amqp = require("amqplib");

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

// async function initializeIndex() {
//     const indexExists = await esClient.indices.exists({ index: "plans" });
//     if (indexExists.statusCode === 404) {
//         // await esClient.indices.create({
//         //     index: "plans",
//         //     body: {
//         //         mappings: {
//         //             properties: {
//         //                 join_field: {
//         //                     type: "join",
//         //                     relations: {
//         //                         plan: ["linkedPlanService", "plancostshare"],
//         //                         linkedPlanService: ["childOfLinkedPlanService"]
//         //                     }
//         //                 }
//         //             }
//         //         }
//         //     }
//         // });
//         await esClient.indices.create({
//             index: "plans",
//             body: {
//                 mappings: {
//                     properties: {
//                         join_field: {
//                             type: "join",
//                             relations: {
//                                 plan: ["linkedPlanService", "plancostshare"],
//                                 linkedPlanService: ["childOfLinkedPlanService"]
//                             }
//                         }
//                     }
//                 }
//             }
//         });

//         console.log("✅ Created 'plans' index with parent-child mapping.");
//     } else {
//         console.log("ℹ️ 'plans' index already exists.");
//     }
// }

// async function initializeIndex() {
//     // const indexExists = await esClient.indices.exists({ index: "plans" });
//     // if (!indexExists.body) {
//     //     console.log("⏳ Creating new 'plans' index with join mapping...");
//     //     await esClient.indices.create({
//     //         index: "plans",
//     //         body: {
//     //             mappings: {
//     //                 properties: {
//     //                     join_field: {
//     //                         type: "join",
//     //                         relations: {
//     //                             plan: ["linkedPlanService", "plancostshare"],
//     //                             linkedPlanService: ["childOfLinkedPlanService"]
//     //                         }
//     //                     }
//     //                 }
//     //             }
//     //         }
//     //     });
//     //     console.log("✅ Created 'plans' index with parent-child mapping.");
//     // } else {
//     //     console.log("ℹ️ 'plans' index already exists.");
//     // }
//     try {
//         const indexExists = await esClient.indices.exists({ index: "plans" });
//         if (!indexExists.body) {
//             console.log("Creating new 'plans' index with join mapping...");
//             await esClient.indices.create({
//                 index: "plans",
//                 body: {
//                     mappings: {
//                         properties: {
//                             join_field: {
//                                 type: "join",
//                                 relations: {
//                                     plan: ["linkedPlanService", "plancostshare"],
//                                     linkedPlanService: ["childOfLinkedPlanService"]
//                                 }
//                             }
//                         }
//                     }
//                 }
//             });
//             console.log("Created 'plans' index with parent-child mapping.");
//         } else {
//             console.log("'plans' index already exists.");
//         }
//     } catch (err) {
//         if (
//             err.meta &&
//             err.meta.body &&
//             err.meta.body.error &&
//             err.meta.body.error.type === "resource_already_exists_exception"
//         ) {
//             console.warn("Tried to create 'plans' index, but it already exists.");
//         } else {
//             console.error("Error creating 'plans' index:", err);
//         }
//     }

// }
initializeIndex();


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

// Google OAuth2 Authentication
const client = new OAuth2Client(
    "203784104200-4k5d6qa4cilha0500i83a09bqmj8dbf3.apps.googleusercontent.com"
);

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



const { createHash } = require("crypto");

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
    await esClient.delete({ index: "plans", id: planId });

    res.status(204).end();
});

// SEARCH: Query Data in Elasticsearch
app.get("/v1/search", verifyGoogleToken, async (req, res) => {
    const query = req.query.q;

    const result = await esClient.search({
        index: "plans",
        body: {
            query: {
                match: { planType: query }
            }
        }
    });

    res.json(result.hits.hits.map(hit => hit._source));
});

// RabbitMQ Consumer: Index Data into Elasticsearch
async function consumeQueue() {
    channel.consume("indexingQueue", async (msg) => {
        if (msg !== null) {
            const plan = JSON.parse(msg.content.toString());

            // await esClient.index({
            //     index: "plans",
            //     id: plan.objectId,
            //     body: plan,
            // });
            await esClient.index({
                index: "plans",
                id: data.objectId,
                body: {
                    ...data,
                    join_field: "plan"
                }
            });

            channel.ack(msg);
        }
    });
}
consumeQueue();


async function consumeQueue() {
    if (!channel) {
        console.error("RabbitMQ channel initialized.");
        return;
    }

    channel.consume("indexingQueue", async (msg) => {
        if (msg !== null) {
            const data = JSON.parse(msg.content.toString());


            await esClient.index({
                index: "plans",
                id: data.objectId,
                body: {
                    ...data,
                    join_field: "plan"
                }
            });

            // Index each "linkedPlanService" as a child
            if (data.linkedPlanServices && Array.isArray(data.linkedPlanServices)) {
                for (const service of data.linkedPlanServices) {
                    // await esClient.index({
                    //     index: "plans",
                    //     id: service.objectId,
                    //     routing: data.objectId, // Must route by parent ID
                    //     body: {
                    //         ...service,
                    //         relation: {
                    //             name: "service",
                    //             parent: data.objectId
                    //         }
                    //     }
                    // });

                    await esClient.index({
                        index: "plans",
                        id: service.objectId,
                        routing: data.objectId, // route to parent
                        body: {
                            ...service,
                            join_field: {
                                name: "linkedPlanService",
                                parent: data.objectId
                            }
                        }
                    });
                    await esClient.index({
                        index: "plans",
                        id: `${service.objectId}-child`,
                        routing: service.objectId, // routing must be the parent's ID
                        body: {
                            dummyField: "child doc under linkedPlanService",
                            join_field: {
                                name: "childOfLinkedPlanService",
                                parent: service.objectId
                            }
                        }
                    });

                }
            }

            channel.ack(msg);
        }
    });
}

// Initialize Elasticsearch index for parent-child support
initializeIndex().catch(console.error);

// Ensure the queue is set up before the server starts
setupQueue();

// Start the Express server
app.listen(port, () => {
    console.log(`Server running at http://localhost:${port}`);
});

