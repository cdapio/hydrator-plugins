# CSV Parser Transform


Description
-----------
Parses an input field as a CSV Record into a Structured Record. Supports multi-line CSV Record parsing
into multiple Structured Records. Different formats of CSV Record can be parsed using this plugin.
Supports these CSV Record types: ``DEFAULT``, ``EXCEL``, ``MYSQL``, ``RFC4180``, and ``TDF``.


Configuration
-------------
**format:** Specifies the format of the CSV Record the input should be parsed as.

**field:** Specifies the input field that should be parsed as a CSV Record. 
Input records with a null input field propagate all other fields and set fields that
would otherwise be parsed by the CSVParser to null.

**schema:** Specifies the output schema of the CSV Record.
