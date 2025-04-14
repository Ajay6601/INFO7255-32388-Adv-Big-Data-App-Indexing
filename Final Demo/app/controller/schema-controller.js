import { reportTypeError } from 'ajv/dist/compile/validate/dataType.js';
import * as schemaService from './../service/schema-service.js';
import { setResponse, setError, setCreatedResponse, setNotFoundResponse, setNoContentResponse, setUnauthorizedRequest } from './response-handler.js';
import jwt from 'jsonwebtoken';
import jwksClient from 'jwks-rsa';

const GOOGLE_JWKS_URI = 'https://www.googleapis.com/oauth2/v3/certs';
const GOOGLE_ISSUER = 'https://accounts.google.com';

const client = jwksClient({ jwksUri: GOOGLE_JWKS_URI });

const getKey = (header, callback) => {
    client.getSigningKey(header.kid, (err, key) => {
        if (err) {
            callback(err, null);
        } else {
            const signingKey = key.getPublicKey();
            callback(null, signingKey);
        }
    });
};

const verifyToken = (token, response) => {
    return new Promise((resolve, reject) => {
        jwt.verify(token, getKey, { algorithms: ['RS256'], issuer: GOOGLE_ISSUER }, (err, decoded) => {
            if (err) {
                setUnauthorizedRequest(response);
                reject(new Error('Unauthorized: Invalid token'));
                return;
            } else {
                resolve(decoded);
            }
        });
    });
};

// Controller function to handle creation
export const post = async (request, response) => {
    try {

        const authHeader = request.headers.authorization;
        if (!authHeader || !authHeader.startsWith('Bearer ')) {
            setUnauthorizedRequest(response);
            //response.status(401).json({ error: 'Unauthorized: No token provided' });
            return response.end();
        }
        
        const token = authHeader.split(' ')[1];
        const isValid = await verifyToken(token, response);
        if (!isValid) {
            return response.end();
        }

        const usecaseData = request.body;
        // Calling service function to add data
        const newUsecaseData = await schemaService.addUsecaseData(usecaseData);
        response.setHeader('ETag', newUsecaseData.etag);
        setCreatedResponse(newUsecaseData, response);
    } catch (error) {
        if (!response.headersSent) {
            setError(error, response);
        }
    }
};

// Controller function to handle retrieval of all data
export const get = async (request, response) => {
    try {
        const authHeader = request.headers.authorization;
        if (!authHeader || !authHeader.startsWith('Bearer ')) {
            setUnauthorizedRequest(response);
            return response.end();
        }

        const token = authHeader.split(' ')[1];
        const isValid = await verifyToken(token, response);
        if (!isValid) {
            return response.end();
        }
        
        // Calling service function to retrieve all data
        const usecaseData = await schemaService.getAllUsecaseData();
        if (usecaseData.length === 0) {
            setNotFoundResponse(response);
        } else {
            setResponse(usecaseData, response);
        }
    } catch (error) {
        if (!response.headersSent) {
            setError(error, response);
        }
    }
}

export const getById = async (request, response) => {
    try {
        const authHeader = request.headers.authorization;
        if (!authHeader || !authHeader.startsWith('Bearer ')) {
            setUnauthorizedRequest(response);
            return response.end();
        }

        const token = authHeader.split(' ')[1];
        const isValid = await verifyToken(token, response);
        if (!isValid) {
            return response.end();
        }

        const { id } = request.params;
        const etag = request.headers['if-none-match'];
        const result = await schemaService.getDataById(id, etag);
        if (result.status === 304) {
            return response.status(304).end();
        } 
        else if (result.status === 200) {
            response.setHeader('ETag', result.etag);
            return setResponse(result.data, response);
        } else{
            setNotFoundResponse(response);
        }
    } catch (error) {
        if (!response.headersSent) {
            setError(error, response);
        }
    }
}

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
        const authHeader = request.headers.authorization;
        if (!authHeader || !authHeader.startsWith('Bearer ')) {
            setUnauthorizedRequest(response);
            return response.end();
        }

        const token = authHeader.split(' ')[1];
        const isValid = await verifyToken(token, response);
        if (!isValid) {
            return response.end();
        }

        const { id } = request.params;
        // Calling service function to delete data by id
        const deleted = await schemaService.deleteUsecaseDataById(id);
        if (!deleted) {
            setNotFoundResponse(response);
        } else {
            setNoContentResponse(response);
        }
    } catch (error) {
        if (!response.headersSent) {
            setError(error, response);
        }
    }
}


export const patch = async (request, response) => {
    try {
        const authHeader = request.headers.authorization;
        if (!authHeader || !authHeader.startsWith('Bearer ')) {
            setUnauthorizedRequest(response);
            return response.end();
        }

        const token = authHeader.split(' ')[1];
        const isValid = await verifyToken(token, response);
        if (!isValid) {
            return response.end();
        }

        const { id } = request.params;
        const ifMatch = request.headers['if-match'];
        if (!ifMatch) {
            response.status(428).json({ message: "Precondition Required: Missing If-Match header" });
            return;
        }

        const updatedData = await schemaService.updateUsecaseData(id, request.body.linkedPlanServices, ifMatch);
        
        response.setHeader('ETag', updatedData.etag);
        response.status(200).json(updatedData);
    } catch (error) {
        if (!response.headersSent) {
            setError(error, response);
        }
    }
};