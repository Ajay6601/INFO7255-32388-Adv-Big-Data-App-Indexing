import schemaRouter from './schema-route.js';
import searchRouter from './search-routes.js';

// Defining the initialization function for setting up routes in the Express application
const initializeRoutes = (app) => {
    // Mount the plan data router at the specified base path
    app.use('/v1/plandata',schemaRouter);
    // Mount the search router
    app.use('/v1/search', searchRouter);
}

// Exporting the initialization function
export default initializeRoutes;