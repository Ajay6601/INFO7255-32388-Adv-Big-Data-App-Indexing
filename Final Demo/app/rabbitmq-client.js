import amqp from 'amqplib';
import { indexPlan, indexPlanServices, updatePlan, deletePlanWithServices } from './elasticsearch-client.js';

let channel;
let connection;

// Queue names
const INDEX_QUEUE = 'plan_index_queue';
const UPDATE_QUEUE = 'plan_update_queue';
const DELETE_QUEUE = 'plan_delete_queue';

// Initialize RabbitMQ connection and channels
export const initializeRabbitMQ = async () => {
  try {
    connection = await amqp.connect('amqp://localhost');
    channel = await connection.createChannel();
    
    // Create queues with durable option to ensure messages aren't lost if RabbitMQ restarts
    await channel.assertQueue(INDEX_QUEUE, { durable: true });
    await channel.assertQueue(UPDATE_QUEUE, { durable: true });
    await channel.assertQueue(DELETE_QUEUE, { durable: true });
    
    console.log('RabbitMQ initialized successfully');
    
    // Start consumers
    startConsumers();
    
    return true;
  } catch (error) {
    console.error('Failed to initialize RabbitMQ:', error);
    throw error;
  }
};

// Start message consumers
const startConsumers = () => {
  // Consumer for indexing new plans
  channel.consume(INDEX_QUEUE, async (msg) => {
    try {
      const planData = JSON.parse(msg.content.toString());
      
      // Index the plan (parent document)
      await indexPlan(planData);
      
      // Index the linked services (child documents) if they exist
      if (planData.linkedPlanServices && planData.linkedPlanServices.length > 0) {
        await indexPlanServices(planData.objectId, planData.linkedPlanServices);
      }
      
      channel.ack(msg);
      console.log(`Indexed plan: ${planData.objectId}`);
    } catch (error) {
      console.error('Error processing index message:', error);
      // Negative acknowledgment (requeue the message)
      channel.nack(msg, false, true);
    }
  });
  
  // Consumer for updating plans
  channel.consume(UPDATE_QUEUE, async (msg) => {
    try {
      const { planData, newServices } = JSON.parse(msg.content.toString());
      
      // Update the plan in Elasticsearch
      await updatePlan(planData);
      
      // Index new services if they exist
      if (newServices && newServices.length > 0) {
        await indexPlanServices(planData.objectId, newServices);
      }
      
      channel.ack(msg);
      console.log(`Updated plan: ${planData.objectId}`);
    } catch (error) {
      console.error('Error processing update message:', error);
      channel.nack(msg, false, true);
    }
  });
  
  // Consumer for deleting plans
  channel.consume(DELETE_QUEUE, async (msg) => {
    try {
      const planId = msg.content.toString();
      
      // Delete plan and its services from Elasticsearch (cascaded delete)
      await deletePlanWithServices(planId);
      
      channel.ack(msg);
      console.log(`Deleted plan: ${planId}`);
    } catch (error) {
      console.error('Error processing delete message:', error);
      channel.nack(msg, false, true);
    }
  });
};

// Queue a plan for indexing
export const queuePlanForIndexing = async (planData) => {
  if (!channel) throw new Error('RabbitMQ not initialized');
  
  return channel.sendToQueue(
    INDEX_QUEUE,
    Buffer.from(JSON.stringify(planData)),
    { persistent: true }
  );
};

// Queue a plan for updating
export const queuePlanForUpdate = async (planData, newServices) => {
  if (!channel) throw new Error('RabbitMQ not initialized');
  
  return channel.sendToQueue(
    UPDATE_QUEUE,
    Buffer.from(JSON.stringify({ planData, newServices })),
    { persistent: true }
  );
};

// Queue a plan for deletion
export const queuePlanForDeletion = async (planId) => {
  if (!channel) throw new Error('RabbitMQ not initialized');
  
  return channel.sendToQueue(
    DELETE_QUEUE,
    Buffer.from(planId),
    { persistent: true }
  );
};

// Close RabbitMQ connection
export const closeRabbitMQ = async () => {
  try {
    if (channel) await channel.close();
    if (connection) await connection.close();
    return true;
  } catch (error) {
    console.error('Failed to close RabbitMQ connection:', error);
    throw error;
  }
};

export default {
  initializeRabbitMQ,
  queuePlanForIndexing,
  queuePlanForUpdate,
  queuePlanForDeletion,
  closeRabbitMQ
};