# MongoDB Batch Source

Description
-----------
Reads documents from a MongoDB collection and converts each document into a StructuredRecord with the help
of the specified schema. The user can optionally provide input query, input fields, and splitter classes.

Configuration
-------------
**connectionString:** MongoDB connection string.

**schema:** Specifies the schema of document.

**authConnectionString:** Auxiliary MongoDB connection string.

**inputQuery:** Filter the input collection with a query.

**inputFields:** Projection document that can limit the fields that appear in each document.

**splitterClass:** Name of the splitter class to use.
