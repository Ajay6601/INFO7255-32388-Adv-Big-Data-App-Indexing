// elasticsearchService.js

const { Client } = require('@elastic/elasticsearch');
const esClient = new Client({ node: 'http://localhost:9200' });
const INDEX_NAME = 'plans_v2';  // Name of your index

// Function to create the Elasticsearch index with mappings
const initializeIndex = async () => {
    try {
        const indexExists = await esClient.indices.exists({ index: INDEX_NAME });
        if (!indexExists.body) {
            console.log(`Creating new '${INDEX_NAME}' index with join mapping...`);
            await esClient.indices.create({
                index: INDEX_NAME,
                body: {
                    settings: {
                        index: {
                            number_of_shards: 1,
                            number_of_replicas: 1
                        }
                    },
                    mappings: {
                        properties: {
                            join_field: {
                                type: 'join',
                                relations: {
                                    plan: ['linkedPlanServices', 'planCostShares'],
                                    linkedPlanServices: ['childOfLinkedPlanService']
                                }
                            },
                            _org: {
                                type: 'text',
                                fields: {
                                    keyword: { type: 'keyword', ignore_above: 256 }
                                }
                            },
                            copay: { type: 'integer' },
                            creationDate: { type: 'text', fields: { keyword: { type: 'keyword', ignore_above: 256 } } },
                            deductible: { type: 'integer' },
                            linkedPlanServices: {
                                properties: {
                                    _org: { type: 'text' },
                                    objectId: { type: 'keyword' },
                                    objectType: { type: 'text' }
                                }
                            },
                            linkedService: {
                                properties: {
                                    _org: { type: 'text' },
                                    name: { type: 'text' },
                                    objectId: { type: 'keyword' },
                                    objectType: { type: 'text' }
                                }
                            },
                            name: { type: 'text', fields: { keyword: { type: 'keyword', ignore_above: 256 } } },
                            objectId: { type: 'text', fields: { keyword: { type: 'keyword', ignore_above: 256 } } },
                            objectType: { type: 'text', fields: { keyword: { type: 'keyword', ignore_above: 256 } } },
                            plan: {
                                properties: {
                                    _org: { type: 'text' },
                                    creationDate: { type: 'date', format: 'MM-dd-yyyy' },
                                    objectId: { type: 'keyword' },
                                    objectType: { type: 'text' },
                                    planType: { type: 'text' }
                                }
                            },
                            planCostShares: {
                                properties: {
                                    _org: { type: 'text' },
                                    copay: { type: 'integer' },
                                    deductible: { type: 'integer' },
                                    objectId: { type: 'keyword' },
                                    objectType: { type: 'text' }
                                }
                            },
                            planType: { type: 'text', fields: { keyword: { type: 'keyword', ignore_above: 256 } } },
                            plan_join: {
                                type: 'join',
                                eager_global_ordinals: true,
                                relations: {
                                    linkedPlanServices: ['linkedService', 'planserviceCostShares'],
                                    plan: ['planCostShares', 'linkedPlanServices']
                                }
                            },
                            planserviceCostShares: {
                                properties: {
                                    _org: { type: 'text' },
                                    copay: { type: 'integer' },
                                    deductible: { type: 'integer' },
                                    objectId: { type: 'keyword' },
                                    objectType: { type: 'text' }
                                }
                            }
                        }
                    }
                }
            });
            console.log(`Created '${INDEX_NAME}' index with parent-child mapping.`);
        } else {
            console.log(`'${INDEX_NAME}' index already exists.`);
        }
    } catch (err) {
        console.error(`Error creating '${INDEX_NAME}' index:`, err);
    }
};

// Function to index documents into Elasticsearch
const postDocument = async (plan) => {
    try {
        let MapOfDocuments = {};

        // Convert the plan object into the correct document structure for indexing
        await convertMapToDocumentIndex(plan, '', 'plan', plan.objectId);

        console.log(`Indexing ${Object.keys(MapOfDocuments).length} documents`);

        // Loop through the documents and index them into Elasticsearch
        for (const [key, value] of Object.entries(MapOfDocuments)) {
            const [parentId, objectId] = key.split(':');
            console.log(`Indexing document: ID=${objectId}, parent=${parentId || 'none'}, join_field=`, JSON.stringify(value.join_field));

            // Index the document with the correct parent-child relationship
            await esClient.index({
                index: INDEX_NAME,
                id: objectId,
                routing: parentId || undefined,  // Set routing to parentId for child documents
                body: value
            });
        }

        return { message: 'Document has been posted', status: 200 };
    } catch (e) {
        console.error('Error during indexing:', e);
        if (e.meta && e.meta.body) {
            console.error('Elasticsearch error details:', JSON.stringify(e.meta.body, null, 2));
        }
        return { message: 'Document has not been posted', status: 500 };
    }
};

// Function to convert the plan document to an Elasticsearch-friendly format
const convertMapToDocumentIndex = async (jsonObject, parentId, objectName, parentObjId) => {
    const valueMap = {};
    const map = {};

    for (const [key, value] of Object.entries(jsonObject)) {
        const redisKey = `${jsonObject.objectType}:${parentId}`;
        if (Array.isArray(value)) {
            await convertToList(value, jsonObject.objectId, key, parentObjId);
        } else if (typeof value === 'object' && value !== null) {
            await convertMapToDocumentIndex(value, jsonObject.objectId, key, parentObjId);
        } else {
            valueMap[key] = value;
            map[redisKey] = valueMap;
        }
    }

    // Set join_field for different objects
    if (objectName === 'plan') {
        valueMap['join_field'] = 'plan';
    } else if (objectName === 'planCostShares') {
        valueMap['join_field'] = {
            'name': 'plancostshare',
            'parent': parentId
        };
    } else if (objectName === 'linkedService') {
        return map; // Skip creating a separate document for linkedService
    } else {
        const validJoinNames = ['plancostshare', 'linkedPlanService', 'childOfLinkedPlanService'];
        const normalizedName = objectName.toLowerCase();
        
        if (validJoinNames.includes(normalizedName)) {
            valueMap['join_field'] = {
                'name': normalizedName,
                'parent': parentId
            };
        } else {
            console.log(`Skipping object with unknown join name: ${objectName}`);
            return map;
        }
    }

    const id = `${parentId}:${jsonObject.objectId}`;
    if (jsonObject?.objectId) MapOfDocuments[id] = valueMap;
    return map;
};

// Function to convert an array of objects into Elasticsearch-friendly format
const convertToList = async (jsonArray, parentId, objectName, parentObjId) => {
    const list = [];
    for (let i = 0; i < jsonArray.length; i++) {
        let value = jsonArray[i];
        if (Array.isArray(value)) {
            value = await convertToList(value, parentId, objectName, parentObjId);
        } else if (typeof value === 'object' && value !== null) {
            value = await convertMapToDocumentIndex(value, parentId, objectName, parentObjId);
        }
        list.push(value);
    }
    return list;
};

module.exports = {
    postDocument,
    initializeIndex
};
