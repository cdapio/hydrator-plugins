# SolrSearch Batch Sink


Description
-----------
The SolrSearch Batch Sink takes the structured record from the input source and indexes it into the Solr server using
the collection and idField specified by the user.

The incoming fields from the previous stage in pipelines are mapped to Solr fields. Also, user is able to specify the
mode of the Solr to connect to. For example, Single Node Solr or SolrCloud.

Use Case
--------
SolrSearch Batch Sink is used to write data to the Solr server. For example, the plugin can be used in conjuction
with a stream batch source to parse a file and read its contents in Solr.

Properties
----------
**referenceName:** This will be used to uniquely identify this sink for lineage, annotating metadata, etc.

**solrMode:** Solr mode to connect to. For example, Single Node Solr or SolrCloud.

**solrHost:** The hostname and port for the Solr server. For example, localhost:8983 if Single Node Solr or
zkHost1:2181,zkHost2:2181,zkHost3:2181 for SolrCloud.

**collectionName:** Name of the collection where data will be indexed and stored in Solr.

**idField:** Field that will determine the unique id for the document to be indexed. It must match a field name
in the structured record of the input.

**outputFieldMappings:** List of the input fields to map to the output Solr fields. The key specifies the name of the
field to rename, with its corresponding value specifying the new name for that field.

Conditions
----------
The Solr server should be running prior to creating the application.

All the fields that user wants to index into the Solr server, should be properly declared and defined in Solr's
schema.xml file. The Solr server schema should be properly defined prior to creating the application.

If idField('id') in the input record is NULL, then that particular record will be filtered out.

Example
-------
This example connects to a 'Sinlge Node Solr' server, running locally at the default port of 8983, and writes the
data to the specified collection (test_collection). The data is indexed using the id field coming in the input record
. And also the fieldname 'office address' is mapped to the 'address' field in Solr's index.

    {
      "name": "SolrSearch",
      "type": "batchsink",
        "properties": {
          "solrMode": "SingleNode",
          "solrHost": "localhost:8983",
          "collectionName": "test_collection",
          "idField": "id",
			    "outputFieldMappings": "office address:address"
        }
    }

For example, suppose the SolrSearch sink receives the input record:

    +===================================================================================================+
    | id : STRING | firstname : STRING  | lastname : STRING |  Office Address : STRING  | pincode : INT |
    +===================================================================================================+
    | 100A        | John                | Wagh              |  NE Lakeside              | 480001        |
    | 100B        | Brett               | Lee               |  SE Lakeside              | 480001        |
    +===================================================================================================+

 Once SolrSearch sink plugin execution is completed, all the rows from input data will be indexed in the
 test_collection with the fields id, firstname, lastname, address and pincode.