=================================
Transformations: Python Evaluator
=================================

Plugin to use Python code to write an ETL Transform.

Introduction
-----------
Executes user-provided Python code that transforms one record into another.
Input records are converted into Python dictionaries which can be directly accessed
in Python code. "Python" in this context is `Jython <http://www.jython.org>`__, a Java-implementation of Python.
The transform expects to receive a JSON object as input, which it will
convert back to a record in Java to pass to downstream transforms and sinks.

Use Case
--------
The Python Evaluator transform is used when other transforms cannot meet your needs.
For example, you may want to multiply a field by 1024 and rename it from ``'gigabytes'``
to ``'megabytes'``. Or you might want to convert a timestamp into a human-readable date string.

Getting Started
===============

Following are instructions to build and deploy hydrator transform plugins.

Prerequisites
-------------
To use plugins, you must have CDAP version 3.2.0 or later. 
You can download the CDAP Standalone SDK that includes Hydrator `here <http://cask.co/downloads>`__.

Build Plugin
------------
You can get started with the CDAP-plugin by building directly from the latest source code::

  git clone https://github.com/caskdata/hydrator-plugins.git
  cd hydrator-plugins
  mvn clean package -pl python-evaluator-transform -am

You can build without running tests by using::

  mvn clean package -DskipTests -pl python-evaluator-transform

After the build completes, you will have a JAR for the plugin under the
``python-evaluator-transform/target/`` directory.

Deploy Plugin
-------------
You can deploy the transform plugin using the CDAP CLI::

  cdap > load artifact python-evaluator-transform/target/python-evaluator-transform-<version>.jar \
         config-file python-evaluator-transform/target/python-evaluator-transform-<version>.json


Known Issues
============
The PythonEvaluator transform has a memory leak which affects the CDAP SDK. 
Running it multiple times from within the same JVM in the CDAP SDK will result in an 
OutOfMemoryError: PermGen Space (https://issues.cask.co/browse/CDAP-4222).


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
