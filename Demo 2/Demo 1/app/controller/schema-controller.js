import * as schemaService from './../service/schema-service.js';
import { setResponse, setError, setCreatedResponse, setNotFoundResponse, setNoContentResponse } from './response-handler.js';


// // Controller function to handle creation
// export const post = async (request, response) => {
//     try {
//         const usecaseData = request.body;
//         // Calling service function to add data
//         const newUsecaseData = await schemaService.addUsecaseData(usecaseData);
//         setCreatedResponse(newUsecaseData, response);
//     } catch (error) {
//         setError(error, response);
//     }
// }

export const post = async (request, response) => {
    try {
        const usecaseData = request.body;
        const objectId = usecaseData.objectId; // Extract objectId from request
        const ifNoneMatch = request.headers["if-none-match"]; // Get ETag from request

        // Fetch existing data to compare ETag
        const existingData = await schemaService.getDataById(objectId);

        if (existingData && ifNoneMatch) {
            const currentEtag = existingData.etag;
            response.setHeader("ETag", currentEtag);


            // If the ETags match, return 304 Not Modified
            if (ifNoneMatch === currentEtag) {
                console.log("✅ Returning 304 Not Modified (POST).");
                return response.status(304).end();
            }
        }

        // Save new data (overwrite)
        const newUsecaseData = await schemaService.addUsecaseData(usecaseData);
        response.setHeader("ETag", newUsecaseData.etag);
        setCreatedResponse(newUsecaseData, response);
    } catch (error) {
        setError(error, response);
    }
};


// Controller function to handle retrieval of all data
export const get = async (request, response) => {
    try {
        // Calling service function to retrieve all data
        const usecaseData = await schemaService.getAllUsecaseData();
        if (usecaseData.length === 0) {
            setNotFoundResponse(response);
        } else {
            setResponse(usecaseData, response);
        }
    } catch (error) {
        setError(error, response);
    }
}

export const getById = async (request, response) => {
    try {
        const { id } = request.params;
        const etag = request.headers['if-none-match'];
        const result = await schemaService.getDataById(id, etag);
        if (result.status === 304) {
            response.status(304).end();
        } else if (result.status === 200) {
            response.setHeader('ETag', result.etag);
            setResponse(result.data, response);
        } else {
            setNotFoundResponse(response);
        }
    } catch (error) {
        setError(error, response);
    }
};

// // Controller function to handle deletion of existing data
// export const remove = async (request, response) => {
//     try {
//         // Calling service function to delete data
//         const deleted = await schemaService.deleteUsecaseData();
//         if (!deleted) {
//             //setNotFoundResponse(response);
//         } else {
//             //setNoContentResponse(response);
//         }
//     } catch (error) {
//         setError(error, response);
//     }
// }


// Controller function to handle deletion of an existing data by id
export const removeById = async (request, response) => {
    try {
        const { id } = request.params;
        // Calling service function to delete data by id
        const deleted = await schemaService.deleteUsecaseDataById(id);
        if (!deleted) {
            setNotFoundResponse(response);
        } else {
            setNoContentResponse(response);
        }
    } catch (error) {
        setError(error, response);
    }
}