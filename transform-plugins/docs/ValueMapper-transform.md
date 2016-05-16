# ValueMapper Transform


Description
-----------
Value Mapper is a type of transform that maps string values of a field in the input record to another value.
Mappings are usually stored in another KeyValue dataset. This provides you a simple alternative for mapping data in the
record.


Use Case
--------
Suppose, If you want to replace language codes in the input record field to language description.
     **Source Field name:** language_code
     **Target field name:** language_desc
     **Mappings, Source / Target:** DE/German, ES/Spanish, EN/English, etc.



Properties
----------
**mapping:** Contains three fields as **source field, lookup table name, target field**. Defines mapping of source
field to target field and lookup table name for the specified source field. Here **source field** support only of STRING type.

**defaults:** Contains a key value pair of source field and its default value in case if the input value is
NULL/EMPTY or if the lookup value is not present. If no default value has been provided, then the current input field
value will be assigned to the target field. Value accepted is of STRING NULLABLE type only.


Example
-------
Suppose that user takes the input data(employee details) through the csv file or any other source and wants to apply
value mapper on certain field

    {
        "name": "ValueMapper",
        "type": "transform",
        "properties": {
            "mapping": "designation:designationLookupTableName:designationName",
            "defaults": "designation:DefaultDesignation"
        }
    }


For example, if the transform receives this input record:

    +=========================================================+
    | field name | type                | value                |
    +=========================================================+
    | id         | string              | "1234"               |
    | name       | string              | "John"               |
    | salary     | int                 | 9000                 |
    | designation| string              | "2"                  |
    +=========================================================+

Sample structure for Mapping/Lookup Dataset

    +=======================+
    | key        | value    |
    +=======================+
    | 1          | SE       |
    | 2          | SSE      |
    | 3          | ML       |
    +=======================+

After the transformations from ValueMapper plugin, output will have below structure.
Here destination column values replaced with destinationName by using Lookup Dataset (Key-Value pair):

    +=========================================================+
    | field name      | type                | value           |
    +=========================================================+
    | id              | string              | "1234"          |
    | name            | string              | "John"          |
    | salary          | int                 | 9000            |
    | designationName | string              | "SSE"           |
    +=========================================================+

