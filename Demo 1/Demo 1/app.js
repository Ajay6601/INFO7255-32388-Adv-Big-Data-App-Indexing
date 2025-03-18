// import cors from 'cors';
// import express from 'express';

// // Import custom modules
// import initializeRoutes from "./routes/index.js"

// // Define the initialization function for setting up the Express application
// const initialize = (app) => {
//     app.use(cors());
//     app.use(express.json());
//     app.use(express.urlencoded({ extended: true }));
//     initializeRoutes(app);
// }

// // Export the initialization function for use in other modules
// export default initialize;

import express from "express";
import bodyParser from "body-parser";
import crypto from "crypto";
import { v4 as uuidv4 } from "uuid";
import Ajv from "ajv";
import Redis from "ioredis";
import jwt from "jsonwebtoken";
import { OAuth2Client } from "google-auth-library";

const app = express();
const port = 3002;

const redis = new Redis({
    host: "localhost",
    port: 6379, 
});

app.use(bodyParser.json());

// Initialize Google OAuth2 Client
const client = new OAuth2Client('203784104200-4k5d6qa4cilha0500i83a09bqmj8dbf3.apps.googleusercontent.com');  

// JSON Schema for validation
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

// Helper function to generate ETag
const generateEtag = (data) => {
    const hash = crypto.createHash("md5").update(JSON.stringify(data)).digest("hex");
    return hash;
};

// Middleware for verifying ID tokens with Google IDP
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

// POST: Create a new plan (Stores in Redis)
app.post("/v1/plan", verifyGoogleToken, async (req, res) => {
    const data = req.body;
    if (!validate(data)) {
        return res.status(400).json({ error: "Validation failed", details: validate.errors });
    }

    const etag = generateEtag(data);
    data.etag = etag;

    // Store in Redis (Key: objectId, Value: JSON String)
    await redis.set(data.objectId, JSON.stringify(data));

    res.set({
        "X-Powered-By": "Express",
        "Etag": etag,
        "Content-Type": "application/json",
    });

    return res.status(201).json({ id: data.objectId, message: "Plan created successfully" });
});

// PATCH: Update the plan in Redis
app.patch("/v1/plan/:id", verifyGoogleToken, async (req, res) => {
    const planId = req.params.id;
    const data = req.body;

    // Retrieve the current plan from Redis
    const planData = await redis.get(planId);

    if (!planData) {
        return res.status(404).json({ error: "Plan not found" });
    }

    let plan = JSON.parse(planData);

    // Merge the updated fields into the existing plan
    Object.keys(data).forEach((key) => {
        if (Array.isArray(data[key]) && Array.isArray(plan[key])) {
            // If the field is an array (e.g., linkedPlanServices), merge elements based on objectId
            data[key].forEach((updatedItem) => {
                const existingItemIndex = plan[key].findIndex(item => item.objectId === updatedItem.objectId);
                if (existingItemIndex !== -1) {
                    // Merge updated fields for existing objects
                    plan[key][existingItemIndex] = { ...plan[key][existingItemIndex], ...updatedItem };
                } else {
                    // If new object, add it to the array
                    plan[key].push(updatedItem);
                }
            });
        } else {
            // Directly update scalar or object fields
            plan[key] = data[key];
        }
    });

    // Regenerate the ETag for the updated plan
    const newEtag = generateEtag(plan);
    plan.etag = newEtag;  // Ensure the etag is included in the stored plan

    // Store the updated plan back in Redis
    await redis.set(planId, JSON.stringify(plan));

    res.set({
        "X-Powered-By": "Express",
        "Etag": newEtag,
        "Content-Type": "application/json",
    });

    return res.status(200).json(plan);
});

app.get("/v1/plan/:id", verifyGoogleToken, async (req, res) => {
    const planData = await redis.get(req.params.id);
    
    if (!planData) {
        return res.status(404).json({ error: "Plan not found" });
    }

    const plan = JSON.parse(planData);
    const clientEtag = req.header("If-None-Match");

    if (clientEtag && clientEtag === plan.etag) {
        return res.status(304).end();  // If the ETag matches, return 304 NOT MODIFIED
    }

    res.set({
        "X-Powered-By": "Express",
        "Etag": plan.etag,
        "Content-Type": "application/json",
    });

    return res.status(200).json(plan);  // Return the plan with the ETag
});


// DELETE: Remove a plan from Redis
app.delete("/v1/plan/:id", verifyGoogleToken, async (req, res) => {
    const planId = req.params.id;
    const deleted = await redis.del(planId);

    if (!deleted) {
        return res.status(404).json({ error: "Plan not found" });
    }

    res.set({ "X-Powered-By": "Express" });

    return res.status(204).end();
});


// Start server
app.listen(port, () => {
    console.log(`Server running at http://localhost:${port}`);
});

export default function initialize(app) {
    app.use("/api", app);  // Ensure API routes are loaded
}
