.. meta::
:author: Cask Data, Inc.
    :copyright: Copyright © 2015 Cask Data, Inc.

================
Apache Cassandra
================

.. rubric:: Description

Plugins to use Apache Cassandra as a source or sink

--------------------------------
Sources: Batch: Apache Cassandra
--------------------------------

.. rubric:: Description

Batch source to use Apache Cassandra as a source

.. rubric:: Use Case

This source is used whenever you need to read data from Apache Cassandra.
For example, you may want to read in a column family from Cassandra
and store the data in an HBase table.

.. rubric:: Properties

**partitioner**: The partitioner for the keyspace.

**port**: The rpc port for Cassandra.
Check the configuration to make sure that start_rpc is true in cassandra.yaml

**columnFamily**: The column family or table to select data from.

**keyspace**: The keyspace to select data from.

**initialAddress**: The initial address to connect to.

**username:** The username for the keyspace (if one exists).
If this is not empty, then you must supply a password.

**password:** The password for the keyspace (if one exists).
If this is not empty, then you must supply a username.

**query:** The query to select data on.

**schema:** The schema for the data as it will be formatted in CDAP.

**properties:** Any extra properties to include. The property-value pairs should be comma-separated,
and each property should be separated by a colon from its corresponding value.

.. rubric:: Example

::

  {
   "name": "Cassandra",
      "properties": {
        "partitioner": "org.apache.cassandra.dht.Murmur3Partitioner",
        "initialAddress": "localhost",
        "port": "9160",
        "keyspace": "megacorp",
        "columnFamily": "employees",
        "query": "select * from employees where token(id) > ? and token(id) <= ?"
        "schema": "{
          \"type\":\"record\",
          \"name\":\"etlSchemaBody\",
          \"fields\":[
            {\"name\":\"id\",\"type\":\"long\"},
            {\"name\":\"name\",\"type\":\"string\"},
            {\"name\":\"age\",\"type\":\"int\"}]}"
      }
  }

This example connects to Apache Cassandra, which is running locally, and reads in records in the
specified keyspace (megacorp) and column family (employee) which match the query, in this case, select all records.
All data from the column family will be read on each run.

------------------------------
Sinks: Batch: Apache Cassandra
------------------------------

.. rubric:: Description

Batch sink to use Apache Cassandra as a sink

.. rubric:: Use Case

This sink is used whenever you need to write data into Cassandra.
For example, you may want to parse a file and read its contents into Cassandra,
which you can achieve with a stream batch source and Cassandra as a sink.

.. rubric:: Properties

**partitioner**: The partitioner for the keyspace.

**port**: The rpc port for Cassandra.
Check the configuration to make sure that start_rpc is true in cassandra.yaml

**columnFamily**: The column family or table to inject data into.
Create the column family before starting the adapter.

**keyspace**: The keyspace to inject data into.
Create the keyspace before starting the adapter.

**initialAddress**: The initial address to connect to.

**columns:** A comma-separated list of columns in the column family.
The columns should be listed in the same order as they are stored in the column family.

**primaryKey:** A comma-separated list of primary keys.

.. rubric:: Example

::

  {
   "name": "Cassandra",
      "properties": {
        "partitioner": "org.apache.cassandra.dht.Murmur3Partitioner",
        "port": "9160",
        "keyspace": "megacorp",
        "columnFamily": "employees",
        "initialAddress": "localhost",
        "columns": "fname,lname,age,salary",
        "primaryKey": "fname,lname"
      }
  }

This example connects to Apache Cassandra, which is running locally, and writes the data to
the specified column family (employees), which is in the 'megacorp' keyspace.
This column family has four columns and two primary keys, and Apache Cassandra
uses the default Murmur3 partitioner.

---------------------------------
Sinks: Realtime: Apache Cassandra
---------------------------------

.. rubric:: Description

Realtime sink to use Elasticsearch as a sink

.. rubric:: Use Case

This sink is used whenever you need to write data into Cassandra.
For example, you may want to in realtime collect purchase records
and store them in Cassandra for later access.

.. rubric:: Properties

**columnFamily**: The column family or table to inject data into.
Create the column family before starting the adapter.

**keyspace**: The keyspace to inject data into.
Create the keyspace before starting the adapter.

**addresses**: A comma-separated list of address(es) to connect to.

**username:** The username for the keyspace (if one exists).
If this is not empty, then you must also supply a password.

**password:** The password for the keyspace (if one exists).
If this is not empty, then you must also supply a username.

**columns:** A comma-separated list of columns in the column family.
The columns should be listed in the same order as they are stored in the column family.

**consistencyLevel:** The string representation fo the consistency level for the query.

**compression:** The string representation of the compression for the query.

.. rubric:: Example

::

  {
   "name": "Cassandra",
      "properties": {
        "keyspace": "megacorp",
        "columnFamily": "purchases",
        "addresses": "localhost:9042",
        "columns": "fname,lname,email,price",
        "consistencyLevel": "QUORUM",
        "compression": "NONE"
      }
  }

This example connects to Apache Cassandra, which is running locally, and writes the data to
the specified keypsace (megacorp) and table (purchases).