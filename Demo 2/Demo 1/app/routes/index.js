import schemaRouter from './schema-route.js';

// Defining the initialization function for setting up routes in the Express application
const initializeRoutes = (app) => {
    // Mount the meeting notes router at the specified base path
    app.use('/v1/plandata',schemaRouter);
}

// Exporting the initialization function
export default initializeRoutes;