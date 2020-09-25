# Flatten Transform

## Description
Flatten is a transform plugin that flattens nested data structures.

## Use Case

The transform is used  to convert a nested data structure  into a single flattened record where each key in the record 
is a ```_``` name path to the node in the nested structure. 

Configuration
-------------
**Fields to flatten:** Specifies the list of fields in the input schema to be flattened.

**Level to limit flattening:** Limit flattening to a certain level in nested structures.

**Prefix:** An optional prefix to be used while generating names of the flattened fields. This can be used to fix conflicts that can occur if fields have the same name after flattening. The prefix is added as <prefix>_<flattened_name >.

## Example
For example the input record is as follows:

```
Address
    - Street  
    - City 
        - Name
        - Code
    - State
    - Zip 
```

The output schema will be specified as:

| Field             |
| ----------------  |  
| Address_Street    | 
| Address_City_Name |
| Address_City_Code |
| Address_State     | 
| Address_Zip       | 
