============================================
Hive Batch Source and Sink Plugin Collection
============================================

Introduction
============

This project is a collection of Hive Batch source and sink plugins. Following is list of plugins that are currently available:

- Hive Batch Source,
- Hive Batch Sink,

Getting Started
===============

Following are instructions to build and deploy hydrator Hive plugins.

Prerequisites
-------------

To use plugins, you must have CDAP version 3.2.1 or later. You can download CDAP Standalone that includes Hydrator `here <http://cask.co/downloads>`__

Build Plugins
-------------

You can get started with CDAP-plugins by building directly from the latest source code::

  git clone https://github.com/caskdata/hydrator-plugins.git
  cd hive-plugins
  mvn clean package -pl hive-plugins

After the build completes, you will have a jar for each plugin under the
``hive-plugins/target/`` directory.

Deploy Plugins
--------------

You can deploy transform plugins using the CDAP CLI::

  > load artifact target/hive-plugins-1.1.0-SNAPSHOT-batch.jar \
         config-file resources/plugin/hive-plugin-batch.json

Copy the UI configuration to CDAP installation::

  > cp hive-plugins/resources/ui/*.json $CDAP_HOME/ui/templates/common/

Plugin Descriptions
===================

Hive Batch Source
--------------------

:Id:
      **Hive**
:Type:
      batchsource
:Mode:
      Batch
:Description:
      Reads records from Hive table and converts each record into a StructuredRecord with the help
      of the specified schema if provided or else the table's schema.
:Configuration:
    **metastoreURI:** The URI of Hive metastore in the following format: thrift://<hostname>:<port>.
    Example: thrift://somehost.net:9083

    **tableName:** The name of the hive table.

    **Optional Fields**

    **databaseName:** The name of the database. Defaults to 'default'.

    **filter:** Hive expression filter for scan. This filter must reference only partition columns.
    Values from other columns will cause the pipeline to fail.

    **schema:** Optional schema to use while reading from Hive table. If no schema is provided then the schema of the
    table will be used. Note: If you want to use a hive table which has non-primitive types as a source then you
    should provide a schema here with non-primitive fields dropped else your pipeline will fail.

Hive Batch Sink
------------------

:Id:
      **Hive**
:Type:
      batchsink
:Mode:
      Batch
:Description:
      Converts a StructuredRecord to a HCatRecord and then writes it to an existing hive table.
:Configuration:
    **metastoreURI:** The URI of Hive metastore in the following format: thrift://<hostname>:<port>.
    Example: thrift://somehost.net:9083

    **tableName:** The name of the hive table.

    **Optional Fields**

    **databaseName:** The name of the database. Defaults to 'default'

    **filter:** Hive expression filter for write provided as a JSON Map of key value pairs that describe all of the
    partition keys and values for that partition. For example if the partition column is 'type' then this property
    should specified as {"type": "typeOne"}
    To write multiple partitions simultaneously you can leave this empty, but all of the partitioning columns must
    be present in the data you are writing to the sink.

    **schema:** Optional schema to use while writing to Hive table. If no schema is provided then the schema of the
    table will be used and it should match the schema of the data being written.

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

Apache, Apache HBase, and HBase are trademarks of The Apache Software Foundation. Used with
permission. No endorsement by The Apache Software Foundation is implied by the use of these marks.
