=============
Kafka Plugins
=============

Introduction
============

A Kafka Hydrator Plugin that allows the writing of data to Kafka.

Getting Started
===============

Prerequisites
-------------

To use this plugin, you must have CDAP version 3.2.0 or later.

Build Plugins
-------------

You get started with Hydrator plugins by building directly from the latest source code::

  git clone https://github.com/caskdata/hydrator-plugins.git
  cd hydrator-plugins
  mvn clean package -pl kafka-plugins -am

After the build completes, you will have a jar for each plugin under the
``<plugin-name>/target/`` directory.

Deploy Plugins
--------------

You can deploy the Kafka Real-time plugin using the CDAP CLI::

  > load artifact target/kafka-plugins-<version>.jar \
         config-file target/kafka-plugins-<version>.json
  
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

