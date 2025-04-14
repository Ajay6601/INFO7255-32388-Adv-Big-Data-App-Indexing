import { Client } from '@elastic/elasticsearch';

// Create and configure Elasticsearch client
const elasticClient = new Client({
  node: 'http://localhost:9200',
});

// Initialize Elasticsearch index with proper parent-child mappings
export const initializeElasticsearch = async () => {
  try {
    // Check if index exists
    const indexExists = await elasticClient.indices.exists({ 
      index: 'plandata' 
    });
    
    // If index exists, delete it to ensure proper mapping (for development purposes)
    if (indexExists) {
      console.log('Existing index found. Deleting for clean setup...');
      await elasticClient.indices.delete({ 
        index: 'plandata' 
      });
    }
    
    // Create index with proper mappings for parent-child relationships
    await elasticClient.indices.create({
      index: 'plandata',
      body: {
        mappings: {
          properties: {
            // Base fields
            objectId: { type: 'keyword' },
            objectType: { type: 'keyword' },
            planType: { type: 'keyword' },
            creationDate: { type: 'date', format: 'yyyy-MM-dd||yyyy-MM-dd\'T\'HH:mm:ss.SSSZ||epoch_millis||dd-MM-yyyy' },
            _org: { type: 'keyword' },
            etag: { type: 'keyword' },
            
            // Cost sharing fields
            deductible: { type: 'integer' },
            copay: { type: 'integer' },
            
            // Service fields
            name: { type: 'keyword' },
            
            // Join field for parent-child relationship with multiple types
            join_field: {
              type: 'join',
              relations: {
                plan: ['linkedPlanService', 'planCostShare', 'service', 'serviceCostShare']
              }
            }
          }
        }
      }
    });
    
    console.log('Elasticsearch index created with parent-child mappings');
    return true;
  } catch (error) {
    console.error('Failed to initialize Elasticsearch:', error);
    throw error;
  }
};

// Extract only essential fields for a plan (parent document)
const extractPlanEssentials = (planData) => {
  const { 
    _org, 
    objectId, 
    objectType, 
    planType, 
    creationDate, 
    etag 
  } = planData;
  
  return {
    _org,
    objectId,
    objectType,
    planType,
    creationDate,
    etag,
    join_field: {
      name: 'plan'
    }
  };
};

// Function to index a plan and all related entities
export const indexPlan = async (planData) => {
  try {
    const { objectId, linkedPlanServices, planCostShares } = planData;
    
    // Prepare bulk operations array
    const bulkOperations = [];
    
    // Add plan document (parent) with only essential fields
    bulkOperations.push(
      { index: { _index: 'plandata', _id: objectId } },
      extractPlanEssentials(planData)
    );
    
    // Add plan cost shares if available
    if (planCostShares) {
      // Store simplified PlanCostShares document
      bulkOperations.push(
        { index: { _index: 'plandata', _id: planCostShares.objectId, routing: objectId } },
        {
          _org: planCostShares._org,
          objectId: planCostShares.objectId,
          objectType: planCostShares.objectType,
          deductible: planCostShares.deductible,
          copay: planCostShares.copay,
          join_field: {
            name: 'planCostShare',
            parent: objectId
          }
        }
      );
    }
    
    // Add all linked plan services
    if (linkedPlanServices && linkedPlanServices.length > 0) {
      linkedPlanServices.forEach(service => {
        // Index the service with simplified structure
        bulkOperations.push(
          { index: { _index: 'plandata', _id: service.objectId, routing: objectId } },
          {
            _org: service._org,
            objectId: service.objectId,
            objectType: service.objectType,
            join_field: {
              name: 'linkedPlanService',
              parent: objectId
            }
          }
        );
        
        // Index the linked service details if available
        if (service.linkedService) {
          bulkOperations.push(
            { index: { _index: 'plandata', _id: service.linkedService.objectId, routing: objectId } },
            {
              _org: service.linkedService._org,
              objectId: service.linkedService.objectId,
              objectType: service.linkedService.objectType,
              name: service.linkedService.name,
              join_field: {
                name: 'service',
                parent: objectId
              }
            }
          );
        }
        
        // Index the plan service cost shares if available
        if (service.planserviceCostShares) {
          bulkOperations.push(
            { index: { _index: 'plandata', _id: service.planserviceCostShares.objectId, routing: objectId } },
            {
              _org: service.planserviceCostShares._org,
              objectId: service.planserviceCostShares.objectId,
              objectType: service.planserviceCostShares.objectType,
              deductible: service.planserviceCostShares.deductible,
              copay: service.planserviceCostShares.copay,
              join_field: {
                name: 'serviceCostShare',
                parent: objectId
              }
            }
          );
        }
      });
    }
    
    // Execute bulk operation if we have any operations
    if (bulkOperations.length > 0) {
      const result = await elasticClient.bulk({
        refresh: true,
        body: bulkOperations
      });
      
      console.log(`Indexed plan ${objectId} with all related entities`);
      
      // Check for errors
      if (result.errors) {
        console.error('Bulk indexing had errors:', result.items.filter(item => item.index && item.index.error));
      }
      
      return result;
    }
    
    return { success: true };
  } catch (error) {
    console.error('Failed to index plan with relationships:', error);
    throw error;
  }
};

// Index plan services (child documents)
export const indexPlanServices = async (planId, services) => {
  if (!services || services.length === 0) return true;
  
  try {
    const bulkOperations = [];
    
    services.forEach(service => {
      // Index the service
      bulkOperations.push(
        { index: { _index: 'plandata', _id: service.objectId, routing: planId } },
        {
          _org: service._org,
          objectId: service.objectId,
          objectType: service.objectType,
          join_field: {
            name: 'linkedPlanService',
            parent: planId
          }
        }
      );
      
      // Index the linked service details if available
      if (service.linkedService) {
        bulkOperations.push(
          { index: { _index: 'plandata', _id: service.linkedService.objectId, routing: planId } },
          {
            _org: service.linkedService._org,
            objectId: service.linkedService.objectId,
            objectType: service.linkedService.objectType,
            name: service.linkedService.name,
            join_field: {
              name: 'service',
              parent: planId
            }
          }
        );
      }
      
      // Index the plan service cost shares if available
      if (service.planserviceCostShares) {
        bulkOperations.push(
          { index: { _index: 'plandata', _id: service.planserviceCostShares.objectId, routing: planId } },
          {
            _org: service.planserviceCostShares._org,
            objectId: service.planserviceCostShares.objectId,
            objectType: service.planserviceCostShares.objectType,
            deductible: service.planserviceCostShares.deductible,
            copay: service.planserviceCostShares.copay,
            join_field: {
              name: 'serviceCostShare',
              parent: planId
            }
          }
        );
      }
    });
    
    if (bulkOperations.length > 0) {
      const result = await elasticClient.bulk({
        refresh: true,
        body: bulkOperations
      });
      
      console.log(`Indexed ${services.length} services for plan: ${planId}`);
      
      // Check for errors
      if (result.errors) {
        console.error('Bulk indexing had errors:', result.items.filter(item => item.index && item.index.error));
      }
      
      return result;
    }
    
    return { success: true };
  } catch (error) {
    console.error('Failed to index plan services:', error);
    throw error;
  }
};

// Update function for handling plan updates with parent-child relationships
export const updatePlan = async (planData, newServices = []) => {
  try {
    const { objectId } = planData;
    
    // First, update the plan document with only essential fields
    await elasticClient.update({
      index: 'plandata',
      id: objectId,
      body: {
        doc: extractPlanEssentials(planData)
      },
      refresh: true
    });
    
    // If we have new services, index them
    if (newServices && newServices.length > 0) {
      const bulkOperations = [];
      
      newServices.forEach(service => {
        // Index the service with simplified structure
        bulkOperations.push(
          { index: { _index: 'plandata', _id: service.objectId, routing: objectId } },
          {
            _org: service._org,
            objectId: service.objectId,
            objectType: service.objectType,
            join_field: {
              name: 'linkedPlanService',
              parent: objectId
            }
          }
        );
        
        // Index the linked service details if available
        if (service.linkedService) {
          bulkOperations.push(
            { index: { _index: 'plandata', _id: service.linkedService.objectId, routing: objectId } },
            {
              _org: service.linkedService._org,
              objectId: service.linkedService.objectId,
              objectType: service.linkedService.objectType,
              name: service.linkedService.name,
              join_field: {
                name: 'service',
                parent: objectId
              }
            }
          );
        }
        
        // Index the plan service cost shares if available
        if (service.planserviceCostShares) {
          bulkOperations.push(
            { index: { _index: 'plandata', _id: service.planserviceCostShares.objectId, routing: objectId } },
            {
              _org: service.planserviceCostShares._org,
              objectId: service.planserviceCostShares.objectId,
              objectType: service.planserviceCostShares.objectType,
              deductible: service.planserviceCostShares.deductible,
              copay: service.planserviceCostShares.copay,
              join_field: {
                name: 'serviceCostShare',
                parent: objectId
              }
            }
          );
        }
      });
      
      // Execute bulk operation if we have any operations
      if (bulkOperations.length > 0) {
        const result = await elasticClient.bulk({
          refresh: true,
          body: bulkOperations
        });
        
        console.log(`Updated plan ${objectId} with ${newServices.length} new services`);
        
        // Check for errors
        if (result.errors) {
          console.error('Bulk indexing had errors:', result.items.filter(item => item.index && item.index.error));
        }
      }
    }
    
    return { success: true };
  } catch (error) {
    console.error('Failed to update plan with relationships:', error);
    throw error;
  }
};


// Delete plan and its associated services (cascaded delete)
export const deletePlanWithServices = async (planId) => {
  try {
    // Step 1: Delete all children documents associated with the plan
    const childDeleteResponse = await elasticClient.deleteByQuery({
      index: 'plandata',
      routing: planId,
      refresh: true,
      body: {
        query: {
          has_parent: {
            parent_type: 'plan',
            query: {
              term: {
                _id: planId
              }
            }
          }
        }
      }
    });

    console.log(`Deleted ${childDeleteResponse.deleted} child documents for plan ID: ${planId}`);

    // Step 2: Check if the plan exists and delete it
    try {
      const planExists = await elasticClient.exists({
        index: 'plandata',
        id: planId
      });

      if (planExists) {
        await elasticClient.delete({
          index: 'plandata',
          id: planId,
          refresh: true
        });
        console.log(`Plan deleted with ID: ${planId}`);
      } else {
        console.log(`Plan with ID: ${planId} does not exist or was already deleted`);
      }
    } catch (error) {
      if (error.meta?.statusCode !== 404) {
        console.error(`Failed to check or delete plan ID: ${planId}`, error);
        throw error;
      }
    }

    // Step 3: Double-check for any orphaned child documents and clean up
    const checkResponse = await elasticClient.search({
      index: 'plandata',
      routing: planId,
      body: {
        query: {
          term: { 
            "join_field.parent": planId }
        }
      },
      size: 10
    });

    if (checkResponse.hits?.total?.value > 0) {
      console.log(`Found ${checkResponse.hits.total.value} orphaned child documents, deleting them...`);
      
      await elasticClient.deleteByQuery({
        index: 'plandata',
        refresh: true,
        body: {
          query: {
            term: { "join_field.parent": planId }
          }
        }
      });
    }

    return true;

  } catch (error) {
    if (error.meta?.statusCode === 404) {
      console.log(`Plan with ID: ${planId} does not exist or was already deleted`);
      return true;
    }

    console.error('Failed to delete plan with services:', error);
    throw error;
  }
};

// Enhanced search for plans with parent-child relationships
export const searchPlans = async (query = {}) => {
  try {
    // First, get all plans (parents)
    const plansResponse = await elasticClient.search({
      index: 'plandata',
      body: {
        query: {
          bool: {
            must: [
              { term: { "join_field.name": "plan" } },
              ...(Object.entries(query).map(([key, value]) => ({ term: { [key]: value } })))
            ]
          }
        },
        size: 100
      }
    });

    // If no plans found, return empty results
    if (plansResponse.hits.total.value === 0) {
      return {
        took: plansResponse.took,
        timed_out: plansResponse.timed_out,
        _shards: plansResponse._shards,
        hits: {
          total: { value: 0, relation: "eq" },
          max_score: 0,
          hits: []
        }
      };
    }

    // Format the plans with _type field and only essential fields
    const plans = plansResponse.hits.hits.map(hit => ({
      _index: hit._index,
      _type: "_doc", 
      _id: hit._id,
      _score: hit._score,
      _source: {
        _org: hit._source._org,
        objectId: hit._source.objectId,
        objectType: hit._source.objectType,
        planType: hit._source.planType,
        creationDate: hit._source.creationDate,
        etag: hit._source.etag,
        join_field: hit._source.join_field
      }
    }));

    // For each plan, get its children
    const results = [];
    
    for (const plan of plans) {
      // Add the plan to results first
      results.push(plan);
      
      // Get all children for this plan
      const childrenResponse = await elasticClient.search({
        index: 'plandata',
        body: {
          query: {
            bool: {
              must: [
                { 
                  terms: { 
                    "join_field.name": ["linkedPlanService", "planCostShare", "service", "serviceCostShare"] 
                  } 
                },
                { term: { "join_field.parent": plan._id } }
              ]
            }
          },
          size: 1000 
        }
      });
      
      // Add children to the results
      const children = childrenResponse.hits.hits.map(hit => ({
        _index: hit._index,
        _type: "_doc", 
        _id: hit._id,
        _score: hit._score,
        _routing: plan._id,
        _source: hit._source
      }));
      
      results.push(...children);
    }

    // Format the response similar to Elasticsearch's native response
    return {
      took: plansResponse.took,
      timed_out: plansResponse.timed_out,
      _shards: plansResponse._shards,
      hits: {
        total: { value: results.length, relation: "eq" },
        max_score: 1.0,
        hits: results
      }
    };
  } catch (error) {
    console.error('Failed to search plans with relationships:', error);
    throw error;
  }
};


// Search for a single plan by ID with its children
export const searchPlanById = async (planId) => {
  try {
    // First, get the plan
    const planResponse = await elasticClient.search({
      index: 'plandata',
      body: {
        query: {
          bool: {
            must: [
              { term: { "join_field.name": "plan" } },
              { term: { "objectId": planId } }
            ]
          }
        }
      }
    });

    // Check if plan exists
    if (planResponse.hits.total.value === 0) {
      return {
        took: planResponse.took,
        hits: {
          total: { value: 0, relation: "eq" },
          hits: []
        }
      };
    }

    // Format the plan with _type field
    const plan = {
      _index: planResponse.hits.hits[0]._index,
      _type: "_doc",
      _id: planResponse.hits.hits[0]._id,
      _score: planResponse.hits.hits[0]._score,
      _source: {
        _org: planResponse.hits.hits[0]._source._org,
        objectId: planResponse.hits.hits[0]._source.objectId,
        objectType: planResponse.hits.hits[0]._source.objectType,
        planType: planResponse.hits.hits[0]._source.planType,
        creationDate: planResponse.hits.hits[0]._source.creationDate,
        etag: planResponse.hits.hits[0]._source.etag,
        join_field: planResponse.hits.hits[0]._source.join_field
      }
    };

    // Get all children for this plan
    const childrenResponse = await elasticClient.search({
      index: 'plandata',
      body: {
        query: {
          bool: {
            must: [
              { 
                terms: { 
                  "join_field.name": ["linkedPlanService", "planCostShare", "service", "serviceCostShare"] 
                } 
              },
              { term: { "join_field.parent": planId } }
            ]
          }
        },
        size: 1000
      }
    });
    
    // Format children with _type field
    const children = childrenResponse.hits.hits.map(hit => ({
      _index: hit._index,
      _type: "_doc",
      _id: hit._id,
      _score: hit._score,
      _routing: planId,
      _source: hit._source
    }));
    
    // Combine plan and children
    const results = [plan, ...children];
    
    return {
      took: planResponse.took + childrenResponse.took,
      timed_out: false,
      _shards: planResponse._shards,
      hits: {
        total: { value: results.length, relation: "eq" },
        max_score: 1.0,
        hits: results
      }
    };
  } catch (error) {
    console.error(`Failed to search plan by ID ${planId}:`, error);
    throw error;
  }
};

// Search for services by plan ID
export const searchServicesByPlanId = async (planId) => {
  try {
    const response = await elasticClient.search({
      index: 'plandata',
      body: {
        query: {
          bool: {
            must: [
              { terms: { "join_field.name": ["service", "linkedPlanService"] } },
              { term: { "join_field.parent": planId } }
            ]
          }
        },
        size: 1000 // Adjust as needed
      }
    });
    
    // Format results with _type field
    return response.hits.hits.map(hit => ({
      _index: hit._index,
      _type: "_doc",  // Add _type field
      _id: hit._id,
      _score: hit._score,
      _source: hit._source
    }));
  } catch (error) {
    console.error('Failed to search services by plan ID:', error);
    throw error;
  }
};

// Search plans with range query (e.g., copay > 1)
export const searchPlansByRange = async (field, gt, lt) => {
  try {
    // Build the range query
    const rangeQuery = {};
    if (gt !== undefined) rangeQuery.gt = gt;
    if (lt !== undefined) rangeQuery.lt = lt;
    
    // Run the search
    const response = await elasticClient.search({
      index: 'plandata',
      body: {
        query: {
          bool: {
            must: [
              { term: { "join_field.name": "plan" } },
              { range: { [field]: rangeQuery } }
            ]
          }
        }
      }
    });
    
    // Format the response with _type field and essential fields only
    const plans = response.hits.hits.map(hit => ({
      _index: hit._index,
      _type: "_doc",  // Add _type field
      _id: hit._id,
      _score: hit._score,
      _source: {
        _org: hit._source._org,
        objectId: hit._source.objectId,
        objectType: hit._source.objectType,
        planType: hit._source.planType,
        creationDate: hit._source.creationDate,
        etag: hit._source.etag,
        join_field: hit._source.join_field
      }
    }));
    
    return {
      took: response.took,
      hits: {
        total: response.hits.total,
        hits: plans
      }
    };
  } catch (error) {
    console.error(`Failed to search plans by range ${field}:`, error);
    throw error;
  }
};

export default elasticClient;