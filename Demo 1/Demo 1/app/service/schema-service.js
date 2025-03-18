import redis from './../redis-client.js';
import Ajv from "ajv";
import planSchema from '../model/schema.js';
import crypto from 'crypto';

const ajv = new Ajv();
const validate = ajv.compile(planSchema);

/**
 * Fetch all existing data.
 * @returns 
 */
export const getAllUsecaseData = async () => {
    const keys = await redis.keys('*');
    if (!keys.length) return [];
    
    const usecaseData = await Promise.all(keys.map(async (key) => {
        const data = await redis.get(key);
        return JSON.parse(data);
    }));

    return usecaseData;
}

/**
 * Add a new use case data entry.
 * @param {*} usecaseData 
 * @returns 
 */
// export const addUsecaseData = async (usecaseData) => {
//     if (!validate(usecaseData)) throw new Error("Invalid JSON format");
//     const { objectId } = usecaseData;
//     if (!objectId) throw new Error('objectId is required');

//     await redis.set(objectId, JSON.stringify(usecaseData));
//     return usecaseData;
// }


export const addUsecaseData = async (usecaseData) => {
    if (!validate(usecaseData)) throw new Error("Invalid JSON format");

    const { objectId } = usecaseData;
    if (!objectId) throw new Error("objectId is required");

    const jsonData = JSON.stringify(usecaseData);
    const etag = crypto.createHash("sha256").update(jsonData).digest("base64");

    await redis.set(objectId, jsonData); // Store data in Redis

    return { ...usecaseData, etag }; // Return ETag along with data
};

// /**
//  * Delete all use case data.
//  * @returns 
//  */
// export const deleteUsecaseData = async () => {
//     const keys = await redis.keys('*');
//     if (!keys.length) return null;

//     await redis.del(...keys);
//     return true;
// }

/**
 * Delete data by ID.
 * @param {*} id 
 * @returns 
 */
export const deleteUsecaseDataById = async (id) => {
    const exists = await redis.exists(id);
    if (!exists) return null;

    await redis.del(id);
    return true;
}

/**
 * Fetch all existing data by id
 * @param {*} id 
 * @returns 
 */
// export const getDataById = async (id, etag) => {
//     const data = await redis.get(id);
//     if (!data) return null;
//     const hash = Buffer.from(data).toString('base64');
//     return etag === hash ? { status: 304 } : { status: 200, data: JSON.parse(data), etag: hash };
// };
export const getDataById = async (id, etag) => {
    const data = await redis.get(id);
    if (!data) return { status: 404, data: null };

    // Generate a strong ETag (hash of JSON data)
    const hash = crypto.createHash('sha256').update(data).digest('base64');



    // Compare If-None-Match with generated ETag
    if (etag === hash) {
        console.log("✅ ETag Matched! Sending 304 Not Modified.");

        return { status: 304 };  // Return 304 Not Modified if ETag matches
    }

    return { status: 200, data: JSON.parse(data), etag: hash };
};