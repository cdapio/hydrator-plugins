# ValueMapper Transform


Description
-----------
Value Mapper is a transform plugin that maps string values of a field in the input record to mapping value using mapping dataset.
Mappings are usually stored in KeyValue dataset. Value Mapper provides you a simple alternative for manipulating the input data by using mapping dataset


Use Case
--------
Suppose, If you want to replace language codes in the input record field to language description.
     **Source Field name:** language_code
     **Target field name:** language_desc
     **Mappings, Source / Target:** DE/German, ES/Spanish, EN/English, etc.



Properties
----------
**mapping:** Defines mapping of source field to target field and mapping table name for the specified source field.
Contains three fields separated by ":" as **source field, mapping table name, target field**.
Here **source field** support only of STRING type.

**defaults:** Contains a key value pair of source field and its default value in case if the source field value is
NULL/EMPTY or if the mapping value is not present. If no default value has been provided, then the source field value will be mapped to target field.
Value accepted is of STRING NULLABLE type only.


Example
-------
Suppose user takes the input data(employee details) through stream and wants to apply
value mapper transform plugin on designation field

Plugin JSON Representation will be:

    {
        "name": "ValueMapper",
        "type": "transform",
        "properties": {
            "mapping": "designation:designationLookupTableName:designationName",
            "defaults": "designation:DefaultDesignation"
        }
    }


If the transform receives following input record:

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
Here destination column values will be replaced in destinationName by using Mapping Dataset (Key-Value pair):

    +=========================================================+
    | field name      | type                | value           |
    +=========================================================+
    | id              | string              | "1234"          |
    | name            | string              | "John"          |
    | salary          | int                 | 9000            |
    | designationName | string              | "SSE"           |
    +=========================================================+




