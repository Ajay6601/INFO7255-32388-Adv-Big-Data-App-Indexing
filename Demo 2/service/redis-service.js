import redis from '../redis-client.js';
import crypto from 'crypto';
import Ajv from "ajv";
import planSchema from '../models/schema.js';

const ajv = new Ajv();
const validate = ajv.compile(planSchema);

/**
 * Generate a strong ETag (SHA-256 hash of JSON data).
 * @param {*} data 
 * @returns {string}
 */
const generateEtag = (data) => {
    return crypto.createHash("sha256").update(JSON.stringify(data)).digest("base64");
};

/**
 * Fetch all stored plans from Redis.
 * @returns {Array} - List of all stored plans.
 */
export const getAllPlans = async () => {
    const keys = await redis.keys('*');
    if (!keys.length) return [];
    
    const plans = await Promise.all(keys.map(async (key) => {
        const data = await redis.get(key);
        return JSON.parse(data);
    }));

    return plans;
};

/**
 * Store a plan with ETag.
 * @param {Object} planData - Plan data to store.
 * @returns {Object} - Stored plan with ETag.
 */
export const addPlan = async (planData) => {
    if (!validate(planData)) throw new Error("Invalid JSON format");
    if (!planData.objectId) throw new Error("objectId is required");

    const jsonData = JSON.stringify(planData);
    const etag = generateEtag(jsonData);

    await redis.set(planData.objectId, jsonData);
    return { ...planData, etag };
};

/**
 * Retrieve a plan by ID.
 * @param {String} id - The plan objectId.
 * @param {String} etag - Client's ETag.
 * @returns {Object} - Response status and data.
 */
export const getPlanById = async (id, etag) => {
    const data = await redis.get(id);
    if (!data) return { status: 404, data: null };

    const hash = generateEtag(data);
    if (etag === hash) return { status: 304 };  // Not Modified

    return { status: 200, data: JSON.parse(data), etag: hash };
};

/**
 * Update an existing plan.
 * @param {String} id - The plan objectId.
 * @param {Object} updates - Updated plan data.
 * @returns {Object} - Updated plan with new ETag.
 */
export const updatePlanById = async (id, updates) => {
    const existingData = await redis.get(id);
    if (!existingData) return null;

    let plan = JSON.parse(existingData);
    Object.assign(plan, updates);

    const newEtag = generateEtag(JSON.stringify(plan));
    plan.etag = newEtag;

    await redis.set(id, JSON.stringify(plan));
    return plan;
};

/**
 * Delete a plan by ID.
 * @param {String} id - The plan objectId.
 * @returns {Boolean} - True if deleted, false otherwise.
 */
export const deletePlanById = async (id) => {
    const exists = await redis.exists(id);
    if (!exists) return false;

    await redis.del(id);
    return true;
};
