=============
Elasticsearch
=============

Plugins to use Elasticsearch as a source or sink.

Sources: Batch: Elasticsearch
=============================

Description
-----------
Batch source to use Elasticsearch as a source.

Use Case
--------
This source is used whenever you need to read data from Elasticsearch.
For example, you may want to read in an index and type from Elasticsearch
and store the data in an HBase table.

Properties
----------
**es.host:** The hostname and port for the Elasticsearch instance.

**es.index:** The name of the index to query.

**es.type:** The name of the type where the data is stored.

**query:** The query to use to import data from the specified index/type.
See Elasticsearch for additional query examples.

**schema:** The schema or mapping of the data in Elasticsearch.

Example
-------
::

  {
    "name": "Elasticsearch",
    "properties": {
      "es.host": "localhost:9200",
      "es.index": "megacorp",
      "es.type": "employee",
      "query": "?q=*",
      "schema": "{
        \"type\":\"record\",
        \"name\":\"etlSchemaBody\",
        \"fields\":[
          {\"name\":\"id\",\"type\":\"long\"},
          {\"name\":\"name\",\"type\":\"string\"},
          {\"name\":\"age\",\"type\":\"int\"}]}"
    }
  }

This example connects to Elasticsearch, which is running locally, and reads in records in the
specified index (*megacorp*) and type (*employee*) which match the query to (in this case) select all records.
All data from the index will be read on each run.

Sinks: Batch: Elasticsearch
===========================

Description
-----------
Batch sink to use Elasticsearch as a sink.

Use Case
--------
This sink is used whenever you need to write data into Elasticsearch.
For example, you may want to parse a file and read its contents into Elasticsearch,
which you can achieve with a stream batch source and Elasticsearch as a sink.

Properties
----------
**es.host:** The hostname and port for the Elasticsearch server.

**es.index:** The name of the index where the data will be stored.
If the index does not already exist, it will be created using
Elasticsearch's default properties.

**es.type:** The name of the type where the data will be stored.
If it does not already exist, it will be created.

**es.idField:** The field that will determine the id for the document.
It should match a fieldname in the structured record of the input.

Example
-------
::

  {
   "name": "Elasticsearch",
      "properties": {
        "es.host": "localhost:9200",
        "es.index": "megacorp",
        "es.type": "employee",
        "es.idField": "id"
      }
  }

This example connects to Elasticsearch, which is running locally, and writes the data to
the specified index (*megacorp*) and type (*employee*). The data is indexed using the *id* field
in the record. Each run, the documents will be updated if they are still present in the source.

Sinks: Real-time: Elasticsearch
===============================

Description
-----------
Real-time sink to use Elasticsearch as a sink.

Use Case
--------
This sink is used whenever you need to write data into Elasticsearch.
For example, you may want to read Kafka logs and store them in Elasticsearch
to be able to search on them.

Properties
----------
**es.cluster:** The name of the cluster to connect to.
Defaults to 'elasticsearch'.

**es.transportAddresses:** The addresses for nodes.
Specify the address for at least one node,
and separate others by commas. Other nodes will be sniffed out.

**es.index:** The name of the index where the data will be stored.
If the index does not already exist, it will be created using
Elasticsearch's default properties.

**es.type:** The name of the type where the data will be stored.
If it does not already exist, it will be created.

**es.idField:** The field that will determine the id for the document.
It should match a fieldname in the structured record of the input.
If left blank, Elasticsearch will create a unique id for each document.

.. rubric:: Example

::

  {
   "name": "Elasticsearch",
      "properties": {
        "es.transportAddresses": "localhost:9300",
        "es.index": "logs",
        "es.type": "cdap",
        "es.idField": "ts"
      }
  }

This example connects to Elasticsearch, which is running locally, and writes the data to
the specified index (*logs*) and type (*cdap*). The data is indexed using the timestamp (*ts*) field
in the record.

Integrating with the CDAP UI
============================
This plugin also contains a config file for the CDAP UI in the *resources* directory - **Elasticsearch.json**.
This configuration file greatly improves the experience of configuring Elasticsearch plugins using the CDAP UI.
It chooses appropriate widgets for the various configuration parameters described above. It also enforces a more
natural ordering for these configuration parameters. To use this file, please copy it over to the
*<SDK_DIR>/ui/templates/common* directory in the CDAP SDK or the *<CDAP_INSTALL_DIR>/ui/templates/common* directory
on your CDAP cluster and restart the CDAP UI.

License and Trademarks
======================

Copyright © 2015 Cask Data, Inc.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
in compliance with the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the 
License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, 
either express or implied. See the License for the specific language governing permissions 
and limitations under the License.

Cask is a trademark of Cask Data, Inc. All rights reserved.

Apache, Apache Cassandra, Apache HBase, and HBase are trademarks of The Apache Software Foundation. Used with
permission. No endorsement by The Apache Software Foundation is implied by the use of these marks.
