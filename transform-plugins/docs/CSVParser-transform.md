# CSV Parser Transform


Description
-----------
Parses an input field as a CSV Record into a Structured Record. Supports multi-line CSV Record parsing
into multiple Structured Records. Different formats of CSV Record can be parsed using this plugin.
Supports these CSV Record types: ``DEFAULT``, ``Tab Delimited``, ``Pipe Delimited``.

Configuration
-------------
**format:** Specifies the format of the CSV Record the input should be parsed as.

**delimiter:** Custom delimiter to be used for parsing the fields. The custom delimiter can only be specified by 
selecting the option 'Custom' from the format drop-down. In case of null, defaults to ",".

**field:** Specifies the input field that should be parsed as a CSV Record. 
Input records with a null input field propagate all other fields and set fields that
would otherwise be parsed by the CSVParser to null.

**schema:** Specifies the output schema of the CSV Record.
