============
CDAP-Plugins
============

Introduction
============
The Cask™ Data Application Platform (CDAP) is an integrated, open source application
development platform for the Hadoop ecosystem that provides developers with data and
application abstractions to simplify and accelerate application development, address a
broader range of real-time and batch use cases, and deploy applications into production
while satisfying enterprise requirements.

CDAP is a layer of software running on top of Apache Hadoop® platforms such as the
Cloudera Enterprise Data Hub or the Hortonworks® Data Platform. CDAP provides these
essential capabilities:

- Abstraction of data in the Hadoop environment through logical representations of underlying data;
- Portability of applications through decoupling underlying infrastructures;
- Services and tools that enable faster application creation in development;
- Integration of the components of the Hadoop ecosystem into a single platform; and
- Higher degrees of operational control in production through enterprise best practices.

CDAP exposes developer APIs (Application Programming Interfaces) for creating applications
and accessing core CDAP services. CDAP defines and implements a diverse collection of
services that implement, deploy, run, and manage applications and data on existing Hadoop
infrastructure such as HBase, HDFS, YARN, MapReduce, Hive, and Spark.

You can run applications ranging from simple MapReduce Jobs through complete ETL (extract,
transform, and load) pipelines all the way up to complex, enterprise-scale data-intensive
applications.

Developers can build and test their applications end-to-end in a full-stack, single-node
installation. CDAP can be run either standalone, deployed within the Enterprise or hosted
in the Cloud.


Plugins Repository
==================
The CDAP plugins repository is a related repository that includes source, sink, and
transform plugins intended to be used with CDAP. For information on CDAP plugins, see the
CDAP documentation's section on `CDAP Applications
<http://docs.cdap.io/cdap/current/en/included-applications/etl/index.html>`__.

For questions about CDAP-plugins, please use any of the communication channels listed at
the `CDAP repository <http://github.com/caskdata/cdap>`__. For more information about
CDAP, head to the `CDAP repository <http://github.com/caskdata/cdap>`__.


Getting Started
===============

Prerequisites
-------------
To use CDAP plugins, you must have CDAP version 3.2.0 or later. Prerequisites for the different
sources, sinks, and transforms are included in their individual README files.
  
Building the Plugins
--------------------
You begin with CDAP plugins by building directly from the latest source code::

  git clone https://github.com/caskdata/cdap-plugins.git
  cd cdap-plugins
  mvn clean package

After the build completes, you will have a JAR for each plugin under each
``<plugin-name>/target/`` directory.

**Note:** You can build without running tests using: ``mvn clean install -DskipTests``

Deploy Plugins
--------------
You then deploy plugins using the CDAP CLI::

  > load artifact <target/plugin-jar> config-file <resources/plugin-config>

Example of loading the Cassandra Plugin (from the cassandra-plugins directory)::

  > load artifact target/cassandra-plugins-1.0.0-batch.jar \
         config-file resources/cassandra-plugins-1.0.0-batch.json
  > load artifact target/cassandra-plugins-1.0.0-realtime.jar \
         config-file resources/cassandra-plugins-1.0.0-realtime.json
         

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
