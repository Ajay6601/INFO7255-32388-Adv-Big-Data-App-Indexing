import express from 'express';
import * as schemaController from './../controller/schema-controller.js';

// Create an instance of Express router
const router = express.Router();

// Define routes and their corresponding controller functions
router.route('/')  // Route for handling requests related to all schema operations
    .get(schemaController.get)  // Handle GET requests to retrieve all usecase data
    .post(schemaController.post); // Handle POST requests to create a new usecase data

router.route('/:id')  // Route for handling requests related to a specific usecase data identified by ID
    .get(schemaController.getById)    
    .delete(schemaController.removeById)  // Handle DELETE requests to remove
    .patch(schemaController.patch);

// Export the router for use in other modules
export default router;