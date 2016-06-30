====================================
Hydrator Transform Plugin Collection
====================================

Introduction
============
This project is a collection of useful transformations of data. These plugins are currently available:

- CSV Parser
- CSV Formatter
- JSON Parser
- Clone Record
- Stream Formatter
- Compressor
- Decompressor
- Encoder
- Decoder
- Hasher
- XML to JSON Converter

Getting Started
===============
Follow these instructions to build and deploy Hydrator transform plugins.

Prerequisites
-------------
To use plugins, you must have CDAP version 3.2.0 or later. You can download CDAP Standalone that includes Hydrator `here <http://cask.co/downloads>`__.
 
Build Plugins
-------------
You get started with Hydrator plugins by building directly from the latest source code::

  git clone https://github.com/caskdata/hydrator-plugins.git
  cd hydrator-plugins
  mvn clean package -pl transform-plugins -am

After the build completes, you will have a JAR for each plugin under each
``<plugin-name>/target/`` directory.

Deploy Plugins
--------------
You can deploy the transform plugin using the CDAP CLI::

  cdap > load artifact target/transform-plugins-<version>.jar \
         config-file target/transform-plugins-<version>.json

Plugin Descriptions
===================

CSV Parser
----------
:ID:
  **CSVParser**
:Type:
  Transform
:Mode:
  Batch and
  Realtime
:Description:
  Parses an input field as a CSV Record into a Structured Record. Supports multi-line CSV Record parsing
  into multiple Structured Records. Different formats of CSV Record can be parsed using this plugin.
  Supports these CSV Record types: DEFAULT, EXCEL, MYSQL, RFC4180, and TDF.
:Configuration:
  - **format:** Specifies the format of CSV Record the input should be parsed as
  - **field:** Specifies the input field that should be parsed as CSV Record
  - **schema:** Specifies the output schema of CSV Record

CSV Formatter
-------------
:ID:
  **CSVFormatter**
:Type:
  Transform
:Mode:
  Batch and
  Realtime
:Description:
  Formats a Structured Record as a CSV Record. Supported CSV Record formats are DELIMITED, EXCEL, MYSQL, RFC4180, and TDF. When the format is DELIMITED, one can specify different delimiters that a CSV Record should use for separating fields.
:Configuration:
  - **format:** Specifies the format of the CSV Record to be generated
  - **delimiter:** Specifies the delimiter to be used to generate a CSV Record; this option is available when the format is specified as ``DELIMITED``
  - **schema:** Specifies the output schema. Output schema should only have fields of type ``String``

JSON Parser
-----------
:ID:
  **JSONParser**
:Type:
  Transform
:Mode:
  Batch and
  Realtime
:Description:
  Parses an input field value as a JSON Object. Each record in the input is parsed as a JSON Object and converted into a Structured Record. The Structured Record can specify particular fields that it's interested in, making projections possible.
:Configuration:
  - **field:** Specifies the input field that should be parsed as a CSV Record
  - **schema:** Specifies the output schema for the JSON Record

JSON Formatter
--------------
:ID:
  **JSONFormatter**
:Type:
  Transform
:Mode:
  Batch and
  Realtime
:Description:
  Formats a Structured Record as JSON Object. Plugin will convert the Structured Record to a JSON object and write to the output record. The output record schema is a single field, either type STRING or type BYTE array.
:Configuration:
  **schema:** Specifies the output schema, a single field either type ``STRING`` or type ``BYTE`` array

Clone Record
------------
:ID:
  **CloneRecord**
:Type:
  Transform
:Mode:
  Batch and
  Realtime
:Description:
  Makes a copy of every input record received for a configured number of times on the output. This transform does not change any record fields or types. It's an identity transform.
:Configuration:
  **copies:** Specifies the numbers of copies of the input record that are to be emitted

Stream Formatter
----------------
:ID:
  **StreamFormatter**
:Type:
  Transform
:Mode:
  Batch and
  Realtime
:Description:
  Formats a Structured Record as Stream format. Plugin will convert the Structured Record to Stream format.
  It will include a header and body configurations. The body of the Stream event can be either type CSV or JSON.
:Configuration:
  - **body:** Specifies the input Structured Record fields that should be included in the body of the Stream event
  - **header:** Specifies the input Structured Record fields that should be included in the header of the Stream event
  - **format:** Specifies the format of the body. Currently supported formats are JSON, CSV, TSV, and PSV
  - **schema:** Specifies the output schema; the output schema can have only two fields: one of type ``STRING`` and the other of type ``MAP<STRING, STRING>``

Compressor
----------
:ID:
  **Compressor**
:Type:
  Transform
:Mode:
  Batch and
  Realtime
:Description:
  Compresses configured fields. Multiple fields can be specified to be compressed using different compression algorithms.
  Plugin supports SNAPPY, ZIP, and GZIP types of compression of fields.
:Configuration:
  - **compressor:** Specifies the configuration for compressing fields; in JSON configuration, this is specified as ``<field>:<compressor>[,<field>:<compressor>]*``
  - **schema:** Specifies the output schema; the fields that are compressed will have the same field name but they will be of type ``BYTE`` array

Decompressor
------------
:ID:
  **Decompressor**
:Type:
  Transform
:Mode:
  Batch and
  Realtime
:Description:
    Decompresses configured fields. Multiple fields can be specified to be decompressed using different decompression algorithms.
    Plugin supports SNAPPY, ZIP, and GZIP types of decompression of fields.
:Configuration:
  - **decompressor:** Specifies the configuration for decompressing fields; in JSON configuration, this is specified as ``<field>:<decompressor>[,<field>:<decompressor>]*``
  - **schema:** Specifies the output schema; the fields that are decompressed will have the same field name but they will be of type ``BYTE`` array or ``STRING``

Encoder
-------
:ID:
  **Encoder**
:Type:
  Transform
:Mode:
  Batch and
  Realtime
:Description:
  Encodes configured fields. Multiple fields can be specified to be encoded using different encoding methods.
  Available encoding methods are STRING_BASE64, BASE64, BASE32, STRING_BASE32, and HEX.
:Configuration:
  - **encode:** Specifies the configuration for encode fields; in JSON configuration, this is specified as ``<field>:<encoder>[,<field>:<encoder>]*``
  - **schema:** Specifies the output schema; the fields that are encoded will have the same field name but they will be of type ``BYTE`` array or ``STRING``

Decoder
-------
:ID:
  **Decoder**
:Type:
  Transform
:Mode:
  Batch and
  Realtime
:Description:
  Decodes configured fields. Multiple fields can be specified to be decoded using different decoding methods.
  Available decoding methods are STRING_BASE64, BASE64, BASE32, STRING_BASE32, and HEX.
:Configuration:
  - **decode:** Specifies the configuration for decode fields; in JSON configuration, this is specified as ``<field>:<decoder>[,<field>:<decoder>]*``
  - **schema:** Specifies the output schema; the fields that are decoded will have the same field name but they will be of type ``BYTE`` array or ``STRING``

Hasher
------
:ID:
  **Hasher**
:Type:
    Transform
:Mode:
    Batch and
    Realtime
:Description:
    Hashes fields using a digest algorithm such as MD2, MD5, SHA1, SHA256, SHA384, or SHA512.
:Configuration:
  - **fields:** Specifies the fields to be hashed
  - **hash:** Specifies the hashing algorithm

XMLToJSONConverter
------------------
:ID:
  **XMLToJSON**
:Type:
      Transform
:Mode:
      Batch and
      Realtime
:Description:
      Converts an XML string to a JSON string.
:Configuration:
    - **inputField:** Specifies the field containing the XML string
    - **outputField:** Specifies the field to store the JSON string
    - **schema:** Specifies the output schema; If outputField is not present, it will be added.


License and Trademarks
======================
Copyright © 2016 Cask Data, Inc.

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
