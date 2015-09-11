.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015 Cask Data, Inc.

=============
Elasticsearch
=============

.. rubric:: Description

Plugins to use Elasticsearch as a source or sink

-----------------------------
Sources: Batch: Elasticsearch
-----------------------------

.. rubric:: Description

Batch source to use Elasticsearch as a source

.. rubric:: Use Case

This source is used whenever you need to read data from Elasticsearch.
For example, you may want to read in an index and type from Elasticsearch
and store the data in an HBase table.

.. rubric:: Properties

**es.index:** The name of the index to query.

**es.type:** The name of the type where the data is stored.

**query:** The query to use to import data from the specified index/type.
See Elasticsearch for more query examples.

**es.host:** The hostname and port for the Elasticsearch instance.

**schema:** The schema or mapping of the data in Elasticsearch.

.. rubric:: Example

::

  {
    "name": "Elasticsearch",
    "properties": {
      "es.index": "megacorp",
      "es.type": "employee",
      "es.host": "localhost:9200",
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
specified index (megacorp) and type (employee) which match the query, in this case, select all records.
All data from the index will be read on each run.

-----------------------------
Sinks: Batch: Elasticsearch
-----------------------------

.. rubric:: Description

Batch sink to use Elasticsearch as a sink

.. rubric:: Use Case

This sink is used whenever you need to write data into Elasticsearch.
For example, you may want to parse a file and read its contents into Elasticsearch,
which you can achieve with a stream batch source and Elasticsearch as a sink.

.. rubric:: Properties

**es.index:** The name of the index where the data will be stored.
If the index does not already exist, it will be created using
Elasticsearch's default properties.

**es.type:** The name of the type where the data will be stored.
If it does not already exist, it will be created.

**es.host:** The hostname and port for the Elasticsearch server.

**es.idField:** The field that will determine the id for the document.
It should match a fieldname in the structured record of the input.

.. rubric:: Example

::

  {
   "name": "Elasticsearch",
      "properties": {
        "es.index": "megacorp",
        "es.idField": "id",
        "es.host": "localhost:9200",
        "es.type": "employee"
      }
  }

This example connects to Elasticsearch, which is running locally, and writes the data to
the specified index (megacorp) and type (employee). The data is indexed using the id field
in the record. Each run, the documents will be updated if they are still present in the source.

-----------------------------
Sinks: Realtime: Elasticsearch
-----------------------------

.. rubric:: Description

Realtime sink to use Elasticsearch as a sink

.. rubric:: Use Case

This sink is used whenever you need to write data into Elasticsearch.
For example, you may want to store Kafka logs and store them in Elasticsearch
to be able to search on them.

.. rubric:: Properties

**es.index:** The name of the index where the data will be stored.
If the index does not already exist, it will be created using
Elasticsearch's default properties.

**es.type:** The name of the type where the data will be stored.
If it does not already exist, it will be created.

**es.transportAddresses:** The addresses for nodes.
Specify the address for at least one node,
and separate others by commas. Other nodes will be sniffed out.

**es.cluster:** The name of the cluster to connect to.
Defaults to 'elasticsearch'.

**es.idField:** The field that will determine the id for the document.
It should match a fieldname in the structured record of the input.
If left blank, Elasticsearch will create a unique id for each document.

.. rubric:: Example

::

  {
   "name": "Elasticsearch",
      "properties": {
        "es.index": "logs",
        "es.idField": "ts",
        "es.transportAddresses": "localhost:9300",
        "es.type": "cdap"
      }
  }

This example connects to Elasticsearch, which is running locally, and writes the data to
the specified index (logs) and type (cdap). The data is indexed using the timestamp field
in the record.