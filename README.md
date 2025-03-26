# INFO7255-32388-Adv-Big-Data-App-Indexing

## Course Overview:

INFO 7255 focuses on Big Data architecture for building distributed software systems. The course covers essential topics such as ingesting, storing, indexing, and analyzing large-scale data efficiently. Students will learn how to manage the volume, variety, and velocity of new data points while ensuring low-latency data storage, high-throughput indexing, and near real-time predictive analytics.

## Key Topics Covered:

1. Big Data architecture patterns

2. Data ingestion and validation

3. Storage techniques with minimal write latency

4. Indexing methods supporting logical operations, wildcards, geolocation, joins, and aggregations

5. Near real-time analytics and workflow optimization

6. Schema-less data modeling for extensibility

## 📚 Tech Stack

* Node/Express js (REST API)
* ElasticSearch (Search & Indexing)
* Redis (Key/Value Store)
* JSON Schema (Validation)
* OAuth2 / RS256 (Security)
* Zuul API Gateway


The three demonstrations I built showcase the learnings from this lecture. 


### 📌 Demo 1 
  * REST API to handle structured JSON data
  * CRUD operations (Create, Read, Delete)
  * JSON Schema validation
  * Conditional read support
  * Storage in a key/value store

### 📌 Demo 2 
  CRUD operations with merge/patch support
  Advanced API semantics (conditional read/write)
  Security: Bearer token (RS256) with Google IDP
  Data validation with JSON Schema
  Key/value storage

### 📌 Demo 3 
  CRUD with cascaded delete
  Advanced API semantics
  Parent-Child indexing with ElasticSearch
  Search with joins
  Queueing mechanism
  Security

