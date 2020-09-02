# Lookup transform

Description
----------- 
Performs a lookup of a given field within a lookup dataset by matching it with input dataset and includes the 
field, and it's value in resulting dataset.


Use Case
--------
This transform is used when you need to lookup a reference data set for finding a specific value or multiple 
values for input data to complete the information before it is loaded into the data mart or warehouse. 
This will help to provide complete information that needs to be loaded into the data mart.

Properties
----------
**lookupDataset:** Amongst the inputs connected to the lookup transformation, this determines the input that should be
used as the lookup dataset. This dataset will be loaded into memory and will be broadcast to all executors, 
so it should be smaller of the inputs.Amongst the inputs connected to the lookup transformation, 
this determines the input that should be used as the lookup dataset. This dataset will be loaded into memory 
and will be broadcast to all executors, so it should be smaller of the inputs.
**inputKeyField:** Field in the input schema that should be used as a key in the lookup condition.
**lookupKeyField:** Field in the lookup source that should be used as a key in the lookup condition.
**lookupValueField:** Field in the lookup source that should be returned after the lookup.
**outputField:** Name of the field in which to store the result of the lookup. This field will be added to the output 
schema, and will contain the value of the Lookup Value Field.
**defaultValue:** Default value to use when there is no match in the lookup source. Defaults to null.

Example
-------
In case we have two datasets: customers and phone_numbers. In lookup transform plugin we can set phone_numbers as 
lookup dataset which leaves customer as input dataset.
 
|customer|
|---------|
|customer_id |first_name |last_name |
|1           |John       |Doe       |

|phone_numbers|
|-------------|
|id  |customer_id |phone_number |
|1   |1           | 555-555-555 |
|2   |1           | 333-333-333 |

The output record will have the lookup value field which in our case is phone_number aliased as phone

|customer|
|--------|
|id  |first_name  |last_name |phone_number |
|1   |John        |Doe       |555-555-555  |


