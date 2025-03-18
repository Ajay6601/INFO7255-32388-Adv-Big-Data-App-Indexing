import express from "express";
import { getAllPlans, addPlan, getPlanById, updatePlanById, deletePlanById } from "../service/redis-service.js";
import { OAuth2Client } from "google-auth-library";
import Redis from "ioredis";
const router = express.Router();
import { createHash } from "crypto"; 
const client = new OAuth2Client('203784104200-4k5d6qa4cilha0500i83a09bqmj8dbf3.apps.googleusercontent.com');
import Ajv from "ajv";
import planSchema from "../models/schema.js";

const redis = new Redis({
    host: "localhost",
    port: 6379,
});
const ajv = new Ajv();
const validate = ajv.compile(planSchema);

// Middleware: Verify Google ID Token
const verifyGoogleToken = async (req, res, next) => {
    const token = req.header("Authorization")?.replace("Bearer ", "");

    if (!token) {
        return res.status(401).json({
            "message": "Failed to validate user",
            "status": "failed"
        });
    }

    try {
        const ticket = await client.verifyIdToken({
            idToken: token,
            audience: "203784104200-4k5d6qa4cilha0500i83a09bqmj8dbf3.apps.googleusercontent.com",
        });

        req.user = ticket.getPayload();
        next();
    } catch (error) {
        return res.status(401).json({
            "message": "Failed to validate user",
            "status": "failed"
        });
    }
};


// Routes
router.get("/plans", verifyGoogleToken, async (req, res) => {
    const plans = await getAllPlans();
    return res.status(200).json(plans);
});

router.post("/plan", verifyGoogleToken, async (req, res) => {
    const data = req.body;

    if (!validate(data)) {
        return res.status(400).json({ 
            message: "Invalid JSON format",
            errors: validate.errors 
        });
    }

    const { objectId } = data;

    const existingPlan = await redis.get(objectId);
    if (existingPlan) {
        return res.status(409).json({
            message: "Conflict - Plan already exists",
            status: "failed"
        });
    }

    const etag = createHash("sha256").update(JSON.stringify(data)).digest("base64");
    data.etag = etag;

    await redis.set(objectId, JSON.stringify(data));

    res.set({
        "X-Powered-By": "Express",
        "Etag": etag,
        "Content-Type": "application/json",
    });

    return res.status(201).json({ 
        id: objectId, 
        message: "Plan created successfully" 
    });
});



router.get("/plan/:id", verifyGoogleToken, async (req, res) => {
    const { id } = req.params;
    const clientEtag = req.header("If-None-Match");  
    const planData = await redis.get(id);

    if (!planData) {
        return res.status(404).json({ message: "Plan not found" });
    }

    let plan = JSON.parse(planData);
    
    if (clientEtag && clientEtag === plan.etag) {
        return res.status(304).end();
    }

    res.set("Etag", plan.etag);
    return res.status(200).json(plan);
});


router.patch("/plan/:id", verifyGoogleToken, async (req, res) => {
    const planId = req.params.id;
    const data = req.body;

    // ✅ Require If-Match header for updates
    const clientEtag = req.header("If-Match");
    if (!clientEtag) {
        return res.status(428).json({
            message: "Precondition Required - If-Match header is missing",
            status: "failed"
        });
    }

    // ✅ Retrieve the current plan from Redis
    const planData = await redis.get(planId);
    if (!planData) {
        return res.status(404).json({ message: "Plan not found" });
    }

    let plan = JSON.parse(planData);

    // ✅ Validate ETag before updating
    if (clientEtag !== plan.etag) {
        return res.status(412).json({
            message: "Precondition Failed - ETag does not match",
            status: "failed"
        });
    }

    // ✅ Merge updates into existing plan
    const updatedPlan = { ...plan, ...data };

    // ✅ Validate JSON Schema for PATCH updates
    if (!validate(updatedPlan)) {
        return res.status(400).json({ 
            message: "Invalid JSON format",
            errors: validate.errors 
        });
    }

    // ✅ Generate new ETag for updated plan
    const newEtag = createHash("sha256").update(JSON.stringify(updatedPlan)).digest("base64");

    // ✅ If the data hasn't changed, return `304 Not Modified`
    if (newEtag === plan.etag) {
        return res.status(304).json({
            message: "Plan is already up to date",
            status: "not_modified"
        });
    }

    updatedPlan.etag = newEtag;

    // ✅ Store updated plan in Redis
    await redis.set(planId, JSON.stringify(updatedPlan));

    res.set({
        "X-Powered-By": "Express",
        "Etag": newEtag,
        "Content-Type": "application/json",
    });

    return res.status(200).json(updatedPlan);
});


router.delete("/plan/:id", verifyGoogleToken, async (req, res) => {
    const deleted = await deletePlanById(req.params.id);
    if (!deleted) return res.status(404).json({ error: "Plan not found" });

    return res.status(204).end();
});


router.delete("/plan/:id", verifyGoogleToken, async (req, res) => {
    const planId = req.params.id;
    const deleted = await redis.del(planId);

    if (!deleted) {
        return res.status(404).json({ message: "Plan not found" });
    }

    res.set({ "X-Powered-By": "Express" });
    return res.status(204).end();
});


export default router;
