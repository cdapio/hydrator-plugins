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
  mvn clean package -pl transform-plugins

After the build completes, you will have a jar for each plugin under the
``<plugin-name>/target/`` directory.

Deploy Plugins
--------------

You can deploy transform plugins using the CDAP CLI::

  > load artifact target/transform-plugins-<version>-batch.jar \
         config-file resources/transform-plugins-batch.json
  > load artifact target/transform-plugins-<version>-realtime.jar \
         config-file resources/transform-plugins-realtime.json

Copy the UI configuration to CDAP installation::

  > cp transform-plugins/resources/ui/*.json $CDAP_HOME/ui/templates/common/

Plugin Descriptions
===================

CSV Parser
----------

:Id:
  **CSVParser**
:Type:
  Transform
:Mode:
  Batch and
  Realtime
:Description:
  Parses an input field as CSV Record into a Structured Record. Support multi-line CSV record parsing 
  into multiple structured records. Different formats of CSV record can be parsed using this plugin. 
  Following are different CSV record types that are supported by this plugin: DEFAULT, EXCEL, MYSQL, RFC4180 and TDF.
:Configuration:
  **format:** Specifies the format of CSV Record the input should be parsed as, 
  **field:** Specifies the input field that should be parsed as CSV Record and
  **schema:** Specifies the output schema of CSV Record.
  
CSV Formatter
-------------

:Id:
  **CSVFormatter**
:Type:
  Transform
:Mode:
  Batch and
  Realtime
:Description:
  Formats a structured record as a CSV Record. CSV Record formats supported are DELIMITED, EXCEL, MYSQL, RFC4180 and TDF. When the format is DELIMITED one can specify different delimiters that a CSV record should use for separting fields. 
:Configuration:
  **format:** Specifies the format of the CSV record to be generated,
  **delimiter:** Specifies the delimiter to be used to generate a CSV Record. This option is available when format is specified as DELIMITED and 
  **schema:** Specifies the output schema. Output Schema should have only field of type String. 

JSON Parser
-------------

:Id:
  **JSONParser**
:Type:
  Transform
:Mode:
  Batch and
  Realtime
:Description:
  Parses an input field value as a JSON Object. Each record in the input is parsed as a JSON Object and converted into a Structured Record. The structured record can specify only the fields it's interested in. Meaning projections are possible. 
:Configuration:
  **field:** Specifies the input field that should be parsed as CSV Record and
  **schema:** Specifies the output schema for JSON Record.

JSON Formatter
-------------

:Id:
  JSONFormatter
:Type:
  Transform
:Mode:
  Batch and 
  Realtime
:Description:
  Formats a structured record as JSON Object. Plugin will convert the structured record to JSON object and write to output record. The output record should have only one field of type STRING or BYTE array.
:Configuration:
  **schema:** Specifies the output schema that includes only one of type STRING or BYTE array. 

Clone Record
-------------

:Id:
  CloneRecord
:Type:
  Transform
:Mode:
  Batch and
  Realtime
:Description:
  Clones every input record received 'n' times.
:Configuration:
  **copies:** Specifies numbers of copies of input record that has be emitted. 

Stream Formatter
-------------

:Id:
  StreamFormatter
:Type:
  Transform
:Mode:
  Batch and
  Realtime
:Description:
  Formats a structured record as Stream format. Plugin will convert the structured record to Stream format. It will include header configuration and body configuration. The body of Stream event can be of any of the two types : CSV or JSON.
:Configuration:
  **body:** Specifies the fields from input structured record that should be included in the body of Stream event. 
  **header:** Specifies the fields from input structured record that should be included in the header of Stream event. 
  **format:** Specifies the format of the body. Currently supported formats are JSON, CSV, TSV and PSV.
  **schema:** Specifies the output schema. The output schema can have only two fields. One of type STRING and the other of type MAP<STRING, STRING>.

Compressor
-------------

:Id:
  Compressor
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

