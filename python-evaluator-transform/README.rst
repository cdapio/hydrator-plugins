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
You can get started with the Hydrator plugin by building directly from the latest source code::

  git clone https://github.com/caskdata/hydrator-plugins.git
  cd hydrator-plugins
  mvn clean package -pl python-evaluator-transform

You can build without running tests by using::

  mvn clean package -DskipTests -pl python-evaluator-transform

After the build completes, you will have a JAR for the plugin under the
``python-evaluator-transform/target/`` directory.

Deploy Plugin
-------------
You can deploy the transform plugin using the CDAP CLI::

  cdap > load artifact python-evaluator-transform/target/python-evaluator-transform-1.1.0-SNAPSHOT.jar \
         config-file python-evaluator-transform/resources/plugin/python-evaluator-transform.json

Copy the UI configuration to the CDAP installation::

  $ cp python-evaluator-transform/resources/ui/PythonEvaluator.json $CDAP_HOME/ui/templates/common/

Plugin Description
==================

PythonEvaluatorTransform
------------------------

:Id:
    **PythonEvaluatorTransform**
:Type:
    Transform
:Mode:
    Batch and
    Realtime
:Description:
    Executes user-provided Python code that transforms one record into another.
:Configuration:
    **script:** Python code defining how to transform one record into another. The script must
    implement a function called ``'transform'``, which takes as input a Python dictionary (representing
    the input record), an emitter object, and a context object (which contains CDAP metrics and logger).
    The script can then use the emitter object to emit transformed Python dictionaries.

    For example:

    ``'def transform(record, emitter, context): record['count'] *= 1024; emitter.emit(record)'``

    will scale the ``'count'`` field of ``record`` by 1024.

    **schema:** The schema of output objects. If no schema is given, it is assumed that the output
    schema is the same as the input schema.

Example
-------
::

  {
    "name": "PythonEvaluator",
    "properties": {
      "script": "def transform(record, emitter, context):
                     tax = record['subtotal'] * 0.0975
                     if (tax > 1000.0):
                         context.getMetrics().count('tax.above.1000', 1)
                     if (tax < 0.0):
                         context.getLogger().info('Received record with negative subtotal')
                     emitter.emit({
                         'subtotal': record['subtotal'],
                         'tax': tax,
                         'total': record['subtotal'] + tax,
                     })
                ",
      "schema": "{
        \"type\":\"record\",
        \"name\":\"expanded\",
        \"fields\":[
          {\"name\":\"subtotal\",\"type\":\"double\"},
          {\"name\":\"tax\",\"type\":\"double\"},
          {\"name\":\"total\",\"type\":\"double\"}
        ]
      }"
    }
  }

The transform takes records that have a ``'subtotal'`` field, calculates ``'tax'`` and
``'total'`` fields based on the subtotal, and then returns a record, as a Python dictionary,
containing those three fields.


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
