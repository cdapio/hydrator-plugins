=============
HBase Plugins
=============

Introduction
============

Collection of HBase Hydrator Plugins that allow to read and write data from and to external HBase tables respectively.

Getting Started
===============

Prerequisites
-------------

To use plugins, you must have CDAP version 3.2.0 or later.

Build Plugins
-------------

You can get started with Hydrator plugins by building directly from the latest source code::

  git clone https://github.com/caskdata/hydrator-plugins.git
  cd hydrator-plugins
  mvn clean package -pl hbase-plugins

After the build completes, you will have a jar for each plugin under the
``<plugin-name>/target/`` directory.

Deploy Plugins
--------------

You can deploy HBase plugins using the CDAP CLI::

  > load artifact target/hbase-plugins-<version>-batch.jar \
         config-file resources/plugin/hbase-plugins-batch.json

Copy the UI configuration to CDAP installation::

  > cp hbase-plugins/resources/ui/HBase.json $CDAP_HOME/ui/templates/common/

Plugin description
==================

HBase Source
--------------

:Id:
  HBase
:Type:
  Sink and Source
:Mode:
  Batch
:Description:   
  Kafka producer plugins allows you to convert structured record into CSV or JSON.
  Plugin has the capability to push the data to one or more Kafka topics. It can
  use one of the field value from input to partition the data on topic. The producer
  can also be configured to operate in sync or async mode.
:Configuration:
  - **tableName:** Specifies the name of the table.
  - **zkQuorum:** Specifies the Zookeeper Quorum in the format <hostname>[<:port>][,<hostname>[:<port>]]*
  - **zkClientPort** Specifies the Zookeeper port. If zero then default port 2181 is used.
  - **zkNodeParent** Specifies the Zookeeper path where HBase master maintains .META.
  - **columnFamily** Column family to read or write from. 
  - **rowField** Specifies the name field to be considered as row during read and as well as for writing


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

