import express from 'express';
import * as searchController from '../controller/search-controller.js';

// Create an instance of Express router
const router = express.Router();

// Define search routes
router.get('/plans', searchController.searchPlansController);
router.get('/plans/all', searchController.getAllPlansController);
router.get('/plans/range', searchController.searchPlansByRangeController);
router.get('/plans/:id', searchController.getPlanByIdController);
router.get('/plans/:planId/services', searchController.getPlanServicesController);

// Export the router for use in other modules
export default router;