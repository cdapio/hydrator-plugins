=============
Kafka Plugins
=============

Introduction
============

Collection of Kafka Hydrator Plugins that allow to read and write data from and to Kafka respectively.

Kafka Producer
--------------

:Type:
   Sink
:Mode:
   Realtime
:Id:
   KafkaProducer
:Description:   
   Kafka producer plugins allows you to convert structured record into CSV or JSON. 
   Plugin has the capability to push the data to one or more Kafka topics. It can 
   use one of the field value from input to partition the data on topic. The producer
   can also be configured to operate in sync or async mode.
:UI Configuration:
   Yes

Getting Started
===============

Prerequisites
-------------

To use plugins, you must have CDAP version 3.2.0 or later.

Build Plugins
-------------

You can get started with CDAP-plugins by building directly from the latest source code::

  git clone https://github.com/caskdata/hydrator-plugins.git
  cd hydrator-plugins
  mvn clean package -pl kafka-plugins

After the build completes, you will have a jar for each plugin under the
``<plugin-name>/target/`` directory.

Deploy Plugins
--------------

You can deploy kafka realtime plugins using the CDAP CLI::

  > load artifact target/kafka-plugins-<version>-realtime.jar \
         config-file resources/plugin/kafka-plugins-realtime.json

Copy the UI configuration to CDAP installation::

  > cp kafka-plugins/resources/realtime/ui/*.json $CDAP_HOME/ui/templates/cdap-etl-realtime/

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

