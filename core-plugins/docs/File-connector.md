# File Connection


Description
-----------
Use this connection to browse and sample the local file system.

Properties
----------
**Name:** Name of the connection. Connection names must be unique in a namespace.

**Description:** Description of the connection. 

Path of the connection
----------------------
To browse, get a sample from, or get the specification for this connection through
[Pipeline Microservices](https://cdap.atlassian.net/wiki/spaces/DOCS/pages/975929350/Pipeline+Microservices), the `path`
property is required in the request body. It's an absolute local filesystem path of a file or folder.