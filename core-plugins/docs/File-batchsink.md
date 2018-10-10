# File Sink


Description
-----------
Writes to a filesystem in various formats format.

For the csv, delimited, and tsv formats, each record is written out as delimited text.
Complex types like arrays, maps, and records will be converted to strings using their
``toString()`` Java method, so for practical use, fields should be limited to the
string, long, int, double, float, and boolean types.

All types are supported when using the avro or parquet format.

Properties
----------
**Reference Name:** Name used to uniquely identify this sink for lineage, annotating metadata, etc.

**Path:** Path to write to. For example, /path/to/output

**Path Suffix:** Time format for the output directory that will be appended to the path.
For example, the format 'yyyy-MM-dd-HH-mm' will result in a directory of the form '2015-01-01-20-42'.
If not specified, nothing will be appended to the path."

**Format:** Format to write the records in.
The format must be one of 'json', 'avro', 'parquet', 'csv', 'tsv', or 'delimited'.

**Delimiter:** Delimiter to use if the format is 'delimited'.

**File System Properties:** Additional properties to use with the OutputFormat when reading the data.
