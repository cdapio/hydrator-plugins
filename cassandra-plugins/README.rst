================
Apache Cassandra
================

This project is a collection of Elasticsearch source and sink plugins. These plugins are currently available:

- Cassandra Batch Source
- Cassandra Batch Sink
- Cassandra Real-time Sink

Getting Started
===============

Following are instructions to build and deploy the Hydrator Cassandra plugins.

Prerequisites
-------------

To use the plugins, you must have CDAP version 3.2.0 or later. You can download CDAP Standalone that includes Hydrator `here <http://cask.co/downloads>`__

Apache Cassandra v. 2.1.0 is the only version of Apache Cassandra that the Hydrator plugins support.

Build Plugins
-------------

You get started with Hydrator plugins by building directly from the latest source code::

  git clone https://github.com/caskdata/hydrator-plugins.git
  cd hydrator-plugins
  mvn clean package -pl cassandra-plugins -am

After the build completes, you will have a JAR under the
``cassandra-plugins/target/`` directory.

Deploy Plugins
--------------

You can deploy the plugins using the CDAP CLI::

  > load artifact target/cassandra-plugins-<version>.jar \
         config-file target/cassandra-plugins-<version>.json

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
