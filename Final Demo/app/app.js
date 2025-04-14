import cors from 'cors';
import express from 'express';

// Import custom modules
import initializeRoutes from "./routes/index.js"

// Define the initialization function for setting up the Express application
const initialize = (app) => {
    app.use(cors());
    app.use(express.json());
    app.use(express.urlencoded({ extended: true }));
    initializeRoutes(app);
}

// Export the initialization function for use in other modules
export default initialize;