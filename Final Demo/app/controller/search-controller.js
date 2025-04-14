import { searchPlans, searchPlanById, searchServicesByPlanId, searchPlansByRange } from '../elasticsearch-client.js';
import { setResponse, setError, setNotFoundResponse } from './response-handler.js';

// Search for plans based on query parameters
export const searchPlansController = async (request, response) => {
  try {
    const { planType, objectType, org } = request.query;
    
    // Build Elasticsearch query based on provided parameters
    const query = {};
    
    if (planType) {
      query.planType = planType;
    }
    
    if (objectType) {
      query.objectType = objectType;
    }
    
    if (org) {
      query._org = org;
    }
    
    const results = await searchPlans(query);
    
    if (results.hits.total.value === 0) {
      return setNotFoundResponse(response);
    }
    
    setResponse(results, response);
  } catch (error) {
    setError(error, response);
  }
};

// Get a specific plan by ID with all related entities
export const getPlanByIdController = async (request, response) => {
  try {
    const { id } = request.params;
    
    // Search for the specific plan by objectId with its children
    const results = await searchPlanById(id);
    
    if (results.hits.total.value === 0) {
      return setNotFoundResponse(response);
    }
    
    // Return the full Elasticsearch-like response
    setResponse(results, response);
  } catch (error) {
    setError(error, response);
  }
};


// Get services associated with a specific plan (parent-child relationship)
export const getPlanServicesController = async (request, response) => {
  try {
    const { planId } = request.params;
    
    const services = await searchServicesByPlanId(planId);
    
    if (services.length === 0) {
      return response.status(200).json({ 
        message: 'No services found for this plan',
        services: []
      });
    }
    
    setResponse({ services }, response);
  } catch (error) {
    setError(error, response);
  }
};


// Get all plans with full parent-child relationships
export const getAllPlansController = async (request, response) => {
  try {
    // Get all plans with their children
    const results = await searchPlans({});
    
    if (results.hits.total.value === 0) {
      return setNotFoundResponse(response);
    }
    
    // Return the full Elasticsearch-like response
    setResponse(results, response);
  } catch (error) {
    setError(error, response);
  }
};


// Search plans with range query (e.g., copay > value)
export const searchPlansByRangeController = async (request, response) => {
  try {
    const { field, gt, lt } = request.query;
    
    if (!field) {
      return response.status(400).json({
        error: "Field parameter is required"
      });
    }
    
    const gtValue = gt ? parseInt(gt, 10) : undefined;
    const ltValue = lt ? parseInt(lt, 10) : undefined;
    
    const results = await searchPlansByRange(field, gtValue, ltValue);
    
    if (results.hits.total.value === 0) {
      return setNotFoundResponse(response);
    }
    
    setResponse(results, response);
  } catch (error) {
    setError(error, response);
  }
};