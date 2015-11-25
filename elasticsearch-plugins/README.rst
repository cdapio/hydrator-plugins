===============================================
Elasticsearch Source and Sink Plugin Collection
===============================================

This project is a collection of Elasticsearch source and sink plugins. These plugins are currently available:

- Elasticsearch Batch Source
- Elasticsearch Batch Sink
- Elasticsearch Real-time Sink

Getting Started
===============

Following are instructions to build and deploy the Hydrator Elasticsearch plugins.

Prerequisites
-------------

To use the plugins, you must have CDAP version 3.2.0 or later. You can download CDAP Standalone that includes Hydrator `here <http://cask.co/downloads>`__

Build Plugins
-------------

You get started with Hydrator plugins by building directly from the latest source code::

  git clone https://github.com/caskdata/hydrator-plugins.git
  cd hydrator-plugins
  mvn clean package -pl elasticsearch-plugins

After the build completes, you will have a jar for each plugin under the
``elasticsearch-plugins/target/`` directory.

Deploy Plugins
--------------

You can deploy the plugins using the CDAP CLI::

  > load artifact target/elasticsearch-plugins-1.1.0-SNAPSHOT-batch.jar \
         config-file resources/plugin/elasticsearch-batch-plugins.json

  > load artifact target/elasticsearch-plugins-1.1.0-SNAPSHOT-realtime.jar \
         config-file resources/plugin/elasticsearch-realtime-plugins.json

Copy the UI configuration to the CDAP installation::

  > cp elasticsearch-plugins/resources/ui/*.json $CDAP_HOME/ui/templates/common/

Plugin Descriptions
===================

Elasticsearch Batch Source
--------------------------
:Id:
    **Elasticsearch**
:Type:
    batchsource
:Mode:
    Batch
:Description:
    Pulls documents from Elasticsearch according to the query specified by the user and converts each document
    to a Structured Record with the fields and schema specified by the user. The Elasticsearch server should 
    be running prior to creating the application.

    This source is used whenever you need to read data from Elasticsearch. For example, you may want to read 
    in an index and type from Elasticsearch and store the data in an HBase table.    
    
:Configuration:
    - **es.host:** The hostname and port for the Elasticsearch instance
    - **es.index:** The name of the index to query
    - **es.type:** The name of the type where the data is stored
    - **query:** The query to use to import data from the specified index and type; 
      see Elasticsearch for additional query examples
    - **schema:** The schema or mapping of the data in Elasticsearch
:Example:
    This example connects to Elasticsearch, which is running locally, and reads in records in the
    specified index (*megacorp*) and type (*employee*) which match the query to (in this case) select all records.
    All data from the index will be read on each run::

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
      
Elasticsearch Batch Sink
------------------------
:Id:
    **Elasticsearch**
:Type:
    batchsink
:Mode:
    Batch
:Description:
    Takes the Structured Record from the input source and converts it to a JSON string, then indexes it in
    Elasticsearch using the index, type, and idField specified by the user. The Elasticsearch server should 
    be running prior to creating the application.

    This sink is used whenever you need to write to an Elasticsearch server. For example, you
    may want to parse a file and read its contents into Elasticsearch, which you can achieve
    with a stream batch source and Elasticsearch as a sink.    
    
:Configuration:
    - **es.host:** The hostname and port for the Elasticsearch instance
    - **es.index:** The name of the index where the data will be stored; if the index does not
      already exist, it will be created using Elasticsearch's default properties
    - **es.type:** The name of the type where the data will be stored; if it does not already
      exist, it will be created
    - **es.idField:** The field that will determine the id for the document; it should match a fieldname
      in the Structured Record of the input
:Example:
    This example connects to Elasticsearch, which is running locally, and writes the data to
    the specified index (megacorp) and type (employee). The data is indexed using the id field
    in the record. Each run, the documents will be updated if they are still present in the source::

      {
       "name": "Elasticsearch",
          "properties": {
            "es.host": "localhost:9200",
            "es.index": "megacorp",
            "es.type": "employee",
            "es.idField": "id"
          }
      }      
      

Elasticsearch Real-time Sink
----------------------------
:Id:
    **Elasticsearch**
:Type:
    realtimesink
:Mode:
    Real-time
:Description:
    Takes the Structured Record from the input source and converts it to a JSON string, then indexes it in
    Elasticsearch using the index, type, and idField specified by the user. The Elasticsearch server should 
    be running prior to creating the application.

    This sink is used whenever you need to write data into Elasticsearch.
    For example, you may want to read Kafka logs and store them in Elasticsearch
    to be able to search on them.    
    
:Configuration:
    - **es.cluster:** The name of the cluster to connect to; defaults to *elasticsearch*
    - **es.transportAddresses:** The addresses for nodes; specify the address for at least one node,
      and separate others by commas; other nodes will be sniffed out
    - **es.index:** The name of the index where the data will be stored; if the index does not already exist, 
      it will be created using Elasticsearch's default properties
    - **es.type:** The name of the type where the data will be stored; if it does not already exist, it will be created
    - **es.idField:** The field that will determine the id for the document; it should match a fieldname in the 
      Structured Record of the input; if left blank, Elasticsearch will create a unique id for each document
:Example:
    This example connects to Elasticsearch, which is running locally, and writes the data to
    the specified index (*logs*) and type (*cdap*). The data is indexed using the timestamp (*ts*) field
    in the record.::

      {
       "name": "Elasticsearch",
          "properties": {
            "es.transportAddresses": "localhost:9300",
            "es.index": "logs",
            "es.type": "cdap",
            "es.idField": "ts"
          }
      }  

Integrating with the CDAP UI
============================
This plugin contains a config file for the CDAP UI in the *resources* directory: ``Elasticsearch.json``.
This configuration file greatly improves the experience of configuring Elasticsearch plugins using the CDAP UI.
It chooses appropriate widgets for the various configuration parameters described above. It also enforces a more
natural ordering for these configuration parameters. To use this file, please copy it over to the
``<SDK_DIR>/ui/templates/common`` directory in the CDAP SDK or the ``<CDAP_INSTALL_DIR>/ui/templates/common`` directory
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
