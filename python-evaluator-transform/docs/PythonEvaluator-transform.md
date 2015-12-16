Description
-----------
Executes user-provided Python code that transforms one record into another.

Configuration
-------------
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

Known Issues
------------
The PythonEvaluator transform has a memory leak which affects the CDAP SDK.
Running it multiple times from within the same JVM in the CDAP SDK will result in an
OutOfMemoryError: PermGen Space (https://issues.cask.co/browse/CDAP-4222).
