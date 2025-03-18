import express from "express";
import initialize from "./app.js";  // Import the initialize function

const app = express();
const PORT = 3000;

initialize(app);  // Initialize routes

app.listen(PORT, () => console.log(`Server running on port ${PORT}`));
