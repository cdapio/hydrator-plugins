# Java Transform


Description
-----------
Executes user-provided Java code that transforms one record into zero or more records.
Input records are converted into JSON objects which can be directly accessed in
Java code. The transform expects to receive a JSON object as input, which it can
process and emit zero or more records or emit error using the provided emitter object.


Use Case
--------
The Java transform is used when other transforms cannot meet your needs.
For example, you may want to multiply a field by 1024 and rename it from ``'gigabytes'``
to ``'megabytes'``. Or you might want to convert a timestamp into a human-readable date string.


Properties
----------
**script:** Java code defining how to transform input record into zero or more records. The class must
implement a function called ``'transform'``, which takes as input a JSON object (representing
the input record), an emitter object (to emit zero or more output records), 
and a context object (which encapsulates CDAP metrics, logger, and lookup).
For example:

public void transform(StructuredRecord input, Emitter<StructureRecord> emitter) {
" +
      "}

**schema:** The schema of output objects. If no schema is given, it is assumed that the output
schema is the same as the input schema.

**lookup:** The configuration of the lookup tables to be used in your script.
For example, if lookup table "purchases" is configured, then you will be able to perform
operations with that lookup table in your script: ``context.getLookup('purchases').lookup('key')``
Currently supports ``KeyValueTable``.


