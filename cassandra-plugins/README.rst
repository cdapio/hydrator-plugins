================
Apache Cassandra
================

Plugins to use Apache Cassandra as a source or sink.

Prerequisites
-------------
Apache Cassandra v. 2.1.0 is the only version of Apache Cassandra that the CDAP-plugins support.

Sources: Batch: Apache Cassandra
================================

Description
-----------
Batch source to use Apache Cassandra as a source.

Use Case
--------
This source is used whenever you need to read data from Apache Cassandra.
For example, you may want to read in a column family from Cassandra
and store the data in an HBase table.

Properties
----------
**initialAddress:** The initial address to connect to.

**port:** The RPC port for Cassandra.
Check the configuration to make sure that ``start_rpc`` is true in ``cassandra.yaml``.

**keyspace:** The keyspace to select data from.

**partitioner:** The partitioner for the keyspace.

**username:** The username for the keyspace (if one exists).
If this is not empty, then you must supply a password.

**password:** The password for the keyspace (if one exists).
If this is not empty, then you must supply a username.

**columnFamily:** The column family or table to select data from.

**query:** The query to select data on.

**schema:** The schema for the data as it will be formatted in CDAP.

**properties:** Any extra properties to include. The property-value pairs should be comma-separated,
and each property should be separated by a colon from its corresponding value.

Example
-------
::

  {
   "name": "Cassandra",
      "properties": {
        "initialAddress": "localhost",
        "port": "9160",
        "keyspace": "megacorp",
        "partitioner": "org.apache.cassandra.dht.Murmur3Partitioner",
        "columnFamily": "employees",
        "query": "select * from employees where token(id) > ? and token(id) <= ?",
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
specified keyspace (*megacorp*) and column family (*employee*) which match the query to (in this case) select all records.
All data from the column family will be read on each run.

Sinks: Batch: Apache Cassandra
==============================

Description
-----------
Batch sink to use Apache Cassandra as a sink.

Use Case
--------
This sink is used whenever you need to write data into Cassandra.
For example, you may want to parse a file and read its contents into Cassandra,
which you can achieve with a stream batch source and Cassandra as a sink.

Properties
----------
**initialAddress:** The initial address to connect to.

**port:** The RPC port for Cassandra.
Check the configuration to make sure that ``start_rpc`` is true in ``cassandra.yaml``.

**keyspace:** The keyspace to inject data into.
Create the keyspace before starting the adapter.

**partitioner:** The partitioner for the keyspace.

**columnFamily:** The column family or table to inject data into.
Create the column family before starting the adapter.

**columns:** A comma-separated list of columns in the column family.
The columns should be listed in the same order as they are stored in the column family.

**primaryKey:** A comma-separated list of primary keys.

Example
-------
::

  {
   "name": "Cassandra",
      "properties": {
        "initialAddress": "localhost",
        "port": "9160",
        "keyspace": "megacorp",
        "partitioner": "org.apache.cassandra.dht.Murmur3Partitioner",
        "columnFamily": "employees",
        "columns": "fname,lname,age,salary",
        "primaryKey": "fname,lname"
      }
  }

This example connects to Apache Cassandra, which is running locally, and writes the data to
the specified column family (*employees*), which is in the *megacorp* keyspace.
This column family has four columns and two primary keys, and Apache Cassandra
uses the default *Murmur3* partitioner.


Sinks: Real-time: Apache Cassandra
=================================

Description
-----------

Real-time sink to use Apache Cassandra as a sink.

Use Case
--------

This sink is used whenever you need to write data into Cassandra.
For example, you may want, in real time, to collect purchase records
and store them in Cassandra for later access.

Properties
----------

**addresses:** A comma-separated list of address(es) to connect to.

**keyspace:** The keyspace to inject data into.
Create the keyspace before starting the adapter.

**username:** The username for the keyspace (if one exists).
If this is not empty, then you must also supply a password.

**password:** The password for the keyspace (if one exists).
If this is not empty, then you must also supply a username.

**columnFamily:** The column family or table to inject data into.
Create the column family before starting the adapter.

**columns:** A comma-separated list of columns in the column family.
The columns should be listed in the same order as they are stored in the column family.

**consistencyLevel:** The string representation of the consistency level for the query.

**compression:** The string representation of the compression for the query.

Example
-------

::

  {
   "name": "Cassandra",
      "properties": {
        "addresses": "localhost:9042",
        "keyspace": "megacorp",
        "columnFamily": "purchases",
        "columns": "fname,lname,email,price",
        "consistencyLevel": "QUORUM",
        "compression": "NONE"
      }
  }

This example connects to Apache Cassandra, which is running locally, and writes the data to
the specified keyspace (*megacorp*) and column family (*purchases*).

Integrating with the CDAP UI
============================
This plugin also contains a config file for the CDAP UI in the *resources* directory - **Cassandra.json**.
This configuration file greatly improves the experience of configuring Cassandra plugins using the CDAP UI.
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
