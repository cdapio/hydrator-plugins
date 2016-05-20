# RowDenormalizer


Description
-----------
Converts raw data into denormalized data based on a key column. User is able to specify the list of fields that should be used in the denormalized record, with an option to use an alias for the output field name. For example, 'ADDRESS' in the input is mapped to 'addr' in the output schema. 

Use Case
--------
The transform takes input record that stores a variable set of custom attributes for an entity, denormalizes it on the basis of the key field, and then returns a denormalized table according to the output schema specified by the user.
The denormalized data is easier to query.

Properties
----------
**keyField:** Name of the column in the input record which will be used to group the raw data. For Example, KeyField.

**fieldName:** Name of the column in the input record which contains the names of output schema columns. For example, input records have columns 'KeyField', 'FieldName', 'FieldValue'. 'FieldName' contains 'FirstName', 'LastName', 'Address'. "So output record will have column names as 'FirstName', 'LastName', 'Address'.

**fieldValue:** Name of the column in the input record which contains the values for output schema columns. For example, input records have columns 'KeyField', 'FieldName', 'FieldValue' and the 'FieldValue' column contains 'John', 'Wagh', 'NE Lakeside'. So the output record will have values for columns 'FirstName', 'LastName', 'Address' as 'John', 'Wagh', 'NE Lakeside' respectively.

**outputFields:** List of the output fields with its alias to be included in denormalized output. This is a comma separated list of key-value pairs, where key-value pairs are separated by a colon ':'. For example, 'Firstname:fname,Lastname:lname,Address:addr'.

**numPartitions:** Number of partitions to use when grouping data. If not specified, the execution
framework will decide on the number to use.

Example
-------
The transform takes input records that have columns keyfield, fieldname, fieldvalue, denormalizes it on the basis of keyfield, and then returns a denormalized table according to the output schema specified by the user.

    {
          "name": "RowDenormalizer",
          "type": "batchaggregator",
          "properties": {
            "outputFields": "Firstname:,Lastname:,Address:addr",
            "keyField": "keyfield",
            "fieldName": "fieldname",
            "fieldValue": "fieldvalue"
          }
        }
    }

For example, suppose the aggregator receives the input record:

    +======================================+
    | keyfield  | fieldname   | fieldvalue |
    +======================================+
    | joltie    | Firstname   | John       |
    | joltie    | Lastname    | Wagh       |
    | joltie    | Address     | NE Lakeside|
    +======================================+

Output records will contain all the output fields specified by user:

    +=====================================================+
    | keyfield  | Firstname   | Lastname   |  addr        |
    +=====================================================+
    | joltie    | John        | Wagh       |  NE Lakeside |
    +=====================================================+


