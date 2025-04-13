// import cors from 'cors';
// import express from 'express';

// // Import custom modules
// import initializeRoutes from "./routes/index.js"

// // Define the initialization function for setting up the Express application
// const initialize = (app) => {
//     app.use(cors());
//     app.use(express.json());
//     app.use(express.urlencoded({ extended: true }));
//     initializeRoutes(app);
// }

// // Export the initialization function for use in other modules
// export default initialize;

import express from "express";
import bodyParser from "body-parser";
import router from "./routes/routes.js"; // Import the routes

const app = express();

app.use(bodyParser.json());

// Use the routes
app.use("/v1", router);

export default app;
