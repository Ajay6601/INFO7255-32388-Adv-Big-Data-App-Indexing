// Function to set a successful response with provided data
export const setResponse = (data, response) => {
    response.status(200);
    response.json(data);
}

// // Function to set an error response with provided error object
// export const setError = (err, response) => {
//     console.log(err);
//     response.status(500);
//     response.json({
//         error:{
//             code: 'InternalServerError',
//             message: 'Error occured while processing the request'
//         }
//     })
// }

export const setError = (err, response) => {
    console.error(err);

    // Check if the error is a validation or bad request issue
    if (err.name === "ValidationError" || err.message.includes("Invalid") || err.message.includes("missing")) {
        response.status(400).json({
            error: {
                code: "BadRequest",
                message: err.message || "Need correct datatype"
            }
        });
    } else {
        response.status(500).json({
            error: {
                code: "InternalServerError",
                message: "Error occurred while processing the request"
            }
        });
    }
};


// Function to set a response indicating successful creation of a resource
export const setCreatedResponse = (data, response) => {
    response.status(201);
    response.json(data);
}

// Function to set a response indicating that the requested resource was not found
export const setNotFoundResponse = (response) => {
    response.status(404);
    response.json({
        error: {
            code: 'NotFound',
            message: 'Resource not found'
        }
    });
}

// Function to set a response indicating successful deletion of a resource
export const setNoContentResponse = (response) => {
    response.send('Plan Data deleted successfully');
}