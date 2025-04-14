// Function to set a successful response with provided data
export const setResponse = (data, response) => {
    response.status(200);
    response.json(data);
}

// Function to set an error response with provided error object
export const setError = (err, response) => {
    console.log(err);
    response.status(500);
    response.json({
        error:{
            code: 'InternalServerError',
            message: 'Error occured while processing the request'
        }
    })
}

// Function to set a response indicating successful creation of a resource
export const setCreatedResponse = (data, response) => {
    response.status(201);
    response.json(data);
}

// Function to set a response indicating that the requested resource was not found
export const setNotFoundResponse = (response) => {
    response.status(404).json({
            message: 'Object not found'
    });
}

// Function to set a response indicating successful deletion of a resource
export const setNoContentResponse = (response) => {
    response.status(204).json({
        message: "Plan deleted successfully",
        status: "success"
    });
    //response.send('Plan Data deleted successfully');
}

export const setUnauthorizedRequest = (response) => {
    response.status(401).json({
        message: "failed to validate user",
        status: "failed"
    });
}