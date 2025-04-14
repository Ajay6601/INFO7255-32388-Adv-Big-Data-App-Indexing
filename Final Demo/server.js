// Import necessary modules
import express from 'express';
import initialize from './app/app.js';
import { initializeElasticsearch } from './app/elasticsearch-client.js';
import { initializeRabbitMQ } from './app/rabbitmq-client.js';

// Create an instance of Express application
const app= express();
const PORT = 3000;

const initializeServices = async () => {
    try {
      // Initialize ElasticSearch
      await initializeElasticsearch();
      console.log('ElasticSearch initialized successfully');
      
      // Initialize RabbitMQ
      await initializeRabbitMQ();
      console.log('RabbitMQ initialized successfully');
      
      return true;
    } catch (error) {
      console.error('Failed to initialize services:', error);
      process.exit(1);
    }
  };

// Initialize the Express application with middleware and routes
initialize(app);

// Start the Express server and listen for incoming requests on specified port
//app.listen(PORT,() => console.log(`Server running on port ${PORT}`));
(async () => {
    await initializeServices();
    
    app.listen(PORT, () => console.log(`Server running on port ${PORT}`));
  })();
