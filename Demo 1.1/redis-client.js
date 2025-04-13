import Redis from "ioredis";

// Initialize Redis client
const redis = new Redis({
    host: "localhost",   // Change if Redis is running on a different host
    port: 6379,          // Default Redis port
    retryStrategy: (times) => Math.min(times * 50, 2000), // Retry logic
});

redis.on("connect", () => {
    console.log("Connected to Redis...");
});

redis.on("error", (err) => {
    console.error("Redis Connection Error:", err);
});

// Export the Redis client for use in other modules
export default redis;
