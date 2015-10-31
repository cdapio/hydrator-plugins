====================================
Hydrator Transform Plugin Collection
====================================

Introduction
============

This project is a collection of useful transformations of data. Following is list of plugins
that are currently available:

- CSV Parser,
- CSV Formatter,
- JSON Parser,
- JSON Parser,
- Clone Record,
- Stream Formatter and
- Compressor

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
  mvn clean package

After the build completes, you will have a jar for each plugin under the
``<plugin-name>/target/`` directory.

Deploy Plugins
--------------

You can deploy transform plugins using the CDAP CLI::

  > load artifact target/transform-plugins-1.0-SNAPSHOT-batch.jar \
         config-file resources/transform-plugins.json

Copy the UI configuration to CDAP installation::

  > cp transform-plugins/*.json $CDAP_HOME/ui/templates/common/

Plugin Descriptions
===================

CSV Parser
----------

:Id:
  CSVParser
:Type:
  Transform
:Mode:
  Batch
  Realtime
:Description:
  Parses an input field into a CSV Record. Support multi-line parsing into CSV Record.
  Different formats of CSV record can be generated using this plugin. Following are different
  CSV record types that are supported by this plugin: DEFAULT, EXCEL, MYSQL, RFC4180 and TDF.
  
CSV Formatter
-------------

:Id:
  CSVFormatter
:Type:
  Transform
:Mode:
  Batch
  Realtime
:Description:

JSON Parser
-------------

:Id:
  JSONParser
:Type:
  Transform
:Mode:
  Batch
  Realtime
:Description:

JSON Parser
-------------

:Id:
  JSONParser
:Type:
  Transform
:Mode:
  Batch
  Realtime
:Description:

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

